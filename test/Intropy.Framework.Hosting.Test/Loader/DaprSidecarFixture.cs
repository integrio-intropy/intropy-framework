using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Intropy.Framework.Testing.Delivery;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// A loader behind a real Dapr sidecar, deployed as a production loader is: RabbitMQ as the broker
/// with its own dead-letter queue (the component's <c>enableDeadLetter</c>), daprd 1.18 with a
/// declarative <c>Subscription</c> resource (no Dapr dead-letter topic) and a <c>Resiliency</c>
/// policy that retries inbound deliveries, and the loader itself running on the host, where the
/// sidecar reaches it through <c>host.docker.internal</c>. What the loader is and what its
/// subscription declares is the derived fixture's.
/// </summary>
public abstract class DaprSidecarFixture : IAsyncLifetime
{
    /// <summary>RabbitMQ's dead-letter queue for the loader's subscription queue
    /// (<c>&lt;app-id&gt;-&lt;topic&gt;</c>).</summary>
    public const string DeadLetterQueue = $"dlq-{AppId}-{LoaderHost.Topic}";

    /// <summary>How often the sidecar retries a delivery the loader answered <c>RETRY</c>, before it
    /// hands it back to the broker, which dead-letters it.</summary>
    public const int SidecarRetries = 3;

    /// <summary>The loader's Dapr app id.</summary>
    protected const string AppId = "test-loader";

    private const int DaprHttpPort = 3500;

    private INetwork _network = null!;
    private IContainer _rabbitMq = null!;
    private IContainer _daprd = null!;
    private static readonly HttpClient s_http = new();
    private static readonly JsonSerializerOptions s_web = new(JsonSerializerDefaults.Web);
    private Uri _dapr = null!;

    /// <summary>Whether the sidecar is deployed with the <c>Resiliency</c> policy that retries
    /// inbound deliveries.</summary>
    protected virtual bool WithRetryPolicy => true;

    /// <summary>The loader's declarative <c>Subscription</c> resource.</summary>
    protected abstract string Subscription { get; }

    /// <summary>Starts the loader, listening for the sidecar on <paramref name="appPort"/>.</summary>
    protected abstract Task StartLoaderAsync(int appPort);

    protected abstract ValueTask StopLoaderAsync();

    public async Task InitializeAsync()
    {
        // The sidecar connects to the app channel when it starts: the loader listens first.
        var appPort = AppCallbackDelivery.AvailablePort();
        await StartLoaderAsync(appPort);

        _network = new NetworkBuilder().Build();
        await _network.CreateAsync();

        _rabbitMq = new ContainerBuilder("rabbitmq:3-management")
            .WithNetwork(_network)
            .WithNetworkAliases("rabbitmq")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server startup complete"))
            .Build();
        await _rabbitMq.StartAsync();

        var daprd = new ContainerBuilder("daprio/daprd:1.18.1")
            .WithNetwork(_network)
            .WithExtraHost("host.docker.internal", "host-gateway")
            .WithResourceMapping(Encoding.UTF8.GetBytes(PubSub), "/resources/pubsub.yaml")
            .WithResourceMapping(Encoding.UTF8.GetBytes(Subscription), "/resources/subscription.yaml");
        if (WithRetryPolicy)
            daprd = daprd.WithResourceMapping(Encoding.UTF8.GetBytes(Resiliency), "/resources/resiliency.yaml");
        _daprd = daprd
            .WithCommand("./daprd",
                "--app-id", AppId,
                "--app-port", appPort.ToString(CultureInfo.InvariantCulture),
                "--app-protocol", "grpc",
                "--app-channel-address", "host.docker.internal",
                "--dapr-http-port", DaprHttpPort.ToString(CultureInfo.InvariantCulture),
                // Left unset, the internal API takes a random port, which can be the API's own 50001.
                "--dapr-internal-grpc-port", "50002",
                "--resources-path", "/resources",
                "--log-level", "info")
            .WithPortBinding(DaprHttpPort, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request
                .ForPort(DaprHttpPort).ForPath("/v1.0/healthz").ForStatusCode(HttpStatusCode.NoContent)))
            .Build();
        await _daprd.StartAsync();

        _dapr = new Uri($"http://{_daprd.Hostname}:{_daprd.GetMappedPublicPort(DaprHttpPort)}");
    }

    /// <summary>Publishes a CloudEvent to the loader's topic through the sidecar, as an extractor
    /// does: structured, with its own type, the payload a camelCase JSON object, and the
    /// publisher's trace context as a header.</summary>
    /// <param name="extensions">Extension attributes to put on the event, such as a claim pointer.</param>
    public async Task PublishAsync(string id, string type, string subject, object data, string? traceParent = null,
        IReadOnlyDictionary<string, string>? extensions = null)
    {
        var envelope = JsonSerializer.SerializeToNode(new
        {
            specversion = "1.0", id, source = "urn:test:source", type, subject,
            datacontenttype = "application/json", data
        }, s_web)!.AsObject();
        foreach (var (name, value) in extensions ?? new Dictionary<string, string>())
            envelope[name] = value;
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(_dapr, $"/v1.0/publish/pubsub/{LoaderHost.Topic}"))
        {
            Content = new StringContent(envelope.ToJsonString(), Encoding.UTF8, "application/cloudevents+json")
        };
        if (traceParent is not null)
            request.Headers.Add("traceparent", traceParent);
        using var response = await s_http.SendAsync(request);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>Publishes a CloudEvent whose <c>data</c> member is <paramref name="rawData"/>
    /// verbatim, under <paramref name="dataContentType"/>: for payloads that are not camelCase JSON
    /// objects, where what the sidecar makes of the member is the thing under test.</summary>
    /// <param name="id">The CloudEvent id.</param>
    /// <param name="type">The CloudEvent type, which the subscription routes on.</param>
    /// <param name="subject">The CloudEvent subject.</param>
    /// <param name="rawData">The <c>data</c> member as JSON text — <c>"a,b"</c> for a JSON string,
    /// <c>{"a":1}</c> for an object.</param>
    /// <param name="dataContentType">The <c>datacontenttype</c> attribute.</param>
    public async Task PublishRawAsync(string id, string type, string subject, string rawData,
        string dataContentType)
    {
        var envelope = new JsonObject
        {
            ["specversion"] = "1.0",
            ["id"] = id,
            ["source"] = "urn:test:source",
            ["type"] = type,
            ["subject"] = subject,
            ["datacontenttype"] = dataContentType,
            ["data"] = JsonNode.Parse(rawData),
        };
        using var request = new HttpRequestMessage(HttpMethod.Post,
            new Uri(_dapr, $"/v1.0/publish/pubsub/{LoaderHost.Topic}"))
        {
            Content = new StringContent(envelope.ToJsonString(), Encoding.UTF8, "application/cloudevents+json"),
        };
        using var response = await s_http.SendAsync(request);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>How many messages wait in the broker's dead-letter queue.</summary>
    public async Task<int> DeadLetteredAsync()
    {
        var result = await _rabbitMq.ExecAsync(["rabbitmqctl", "-q", "list_queues", "name", "messages"]);
        foreach (var line in result.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var columns = line.Split('\t');
            if (columns is [DeadLetterQueue, var count])
                return int.Parse(count, CultureInfo.InvariantCulture);
        }

        return 0;
    }

    public async Task DisposeAsync()
    {
        await StopLoaderAsync();
        await _daprd.DisposeAsync();
        await _rabbitMq.DisposeAsync();
        await _network.DisposeAsync();
    }

    private const string PubSub = """
        apiVersion: dapr.io/v1alpha1
        kind: Component
        metadata:
          name: pubsub
        spec:
          type: pubsub.rabbitmq
          version: v1
          metadata:
            - name: connectionString
              value: amqp://guest:guest@rabbitmq:5672
            - name: durable
              value: "true"
            - name: deliveryMode
              value: "2"
            - name: enableDeadLetter
              value: "true"
        """;

    private static readonly string Resiliency = $"""
        apiVersion: dapr.io/v1alpha1
        kind: Resiliency
        metadata:
          name: {AppId}
        scopes:
          - {AppId}
        spec:
          policies:
            retries:
              redeliver:
                policy: constant
                duration: 500ms
                maxRetries: {SidecarRetries}
          targets:
            components:
              pubsub:
                inbound:
                  retry: redeliver
        """;
}
