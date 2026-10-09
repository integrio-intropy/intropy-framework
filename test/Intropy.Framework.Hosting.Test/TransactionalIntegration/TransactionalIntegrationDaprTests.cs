using System.Globalization;
using System.Net;
using System.Text;
using Dapr.Client;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Intropy.Framework.Testing.Adapters;
using Intropy.Framework.Testing.Delivery;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

/// <summary>
/// Startup with a real Dapr/RabbitMQ internal queue and no subscription queue yet. The receive pipeline,
/// publisher, callback and source completion are real; the send pipeline records delivered data.
/// </summary>
[Trait("Category", "Integration")]
public class TransactionalIntegrationDaprTests : IAsyncLifetime
{
    private const string AppId = "test-hop";
    private const string PubSubName = "internal-test-hop";
    private const string Topic = "hop";
    private const string Queue = $"{AppId}-{Topic}";
    private const string Source = "inbox";
    private const int HttpPort = 3500;
    private const int GrpcPort = 50001;

    private INetwork _network = null!;
    private IContainer _rabbit = null!;
    private IContainer? _sidecar;
    private readonly int _appPort = AppCallbackDelivery.AvailablePort();

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().Build();
        await _network.CreateAsync();
        _rabbit = new ContainerBuilder("rabbitmq:3-management")
            .WithNetwork(_network)
            .WithNetworkAliases("rabbitmq")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server startup complete"))
            .Build();
        await _rabbit.StartAsync();
    }

    [Fact]
    public async Task MissingQueue_ProbesBeforeSweeping_AndDeliversTheRealFile()
    {
        using var client = await StartSidecarAsync(withSubscription: true);
        await AssertPublishCanSucceedWithoutAQueueAsync(client);
        var source = new InMemoryFileAdapter().AddFile("ORD-1001.json", "{\"orderId\":\"ORD-1001\"}");
        var sink = new RecordingSendPipeline();
        var completion = new QueueCheckedCompletion(this);
        await using var provider = Services(client, source, sink, completion).BuildServiceProvider();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var summary = await provider.GetRequiredService<TransactionalIntegrationJob<Context>>()
            .ExecuteAsync(deadline.Token);
        var delivered = await sink.Delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal("{\"orderId\":\"ORD-1001\"}", Encoding.UTF8.GetString(delivered));
        Assert.Equal(1, sink.Count); // probes never enter the send pipeline
        Assert.True(completion.QueueExistedAtCompletion);
        Assert.Empty(source.Files);
        Assert.Equal(new RunSummary(Processed: 1, Failed: 0, Skipped: 0), summary);
    }

    [Fact]
    public async Task MissingSubscription_LeavesTheSourceUntouched_AndExitsAsInfrastructureFailure()
    {
        using var client = await StartSidecarAsync(withSubscription: false);
        await AssertPublishCanSucceedWithoutAQueueAsync(client);
        var source = new InMemoryFileAdapter().AddFile("ORD-1001.json", "order");
        // Any source listing/reading would throw: the gate must fail before source I/O begins.
        source.ReadException = new InvalidOperationException("The source must not be touched while probing");
        var sink = new RecordingSendPipeline();
        await using var provider = Services(client, source, sink, FileCompletion.Delete,
            internalQueueReadyTimeout: TimeSpan.FromSeconds(2)).BuildServiceProvider();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var exitCode = await provider.GetRequiredService<JobRunner>().RunAsync(deadline.Token);

        Assert.Equal(JobExitCodes.InfrastructureFailure, exitCode);
        Assert.True(source.Files.ContainsKey("ORD-1001.json"));
        Assert.Equal(0, sink.Count);
        Assert.False(await QueueExistsAsync());
    }

    private ServiceCollection Services(DaprClient client, IFileAdapter source, ISendPipeline<Context> sink,
        FileCompletion completion, TimeSpan? internalQueueReadyTimeout = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(client);
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = AppId;
            options.ServiceNamespace = "test";
        });
        services.AddKeyedSingleton(Source, source);
        services.AddSingleton(sink);
        services.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>
        {
            SourcePort = Source,
            Completion = completion,
            Configure = options =>
            {
                options.CallbackPort = _appPort;
                options.IdleTimeout = TimeSpan.FromSeconds(1);
                options.PostIdleGracePeriod = TimeSpan.FromSeconds(5);
                options.InternalQueueReadyTimeout = internalQueueReadyTimeout ?? TimeSpan.FromSeconds(30);
                options.InternalQueueProbeInterval = TimeSpan.FromMilliseconds(100);
            }
        });
        return services;
    }

    private async Task<DaprClient> StartSidecarAsync(bool withSubscription)
    {
        var builder = new ContainerBuilder("daprio/daprd:1.18.1")
            .WithNetwork(_network)
            .WithExtraHost("host.docker.internal", "host-gateway")
            .WithResourceMapping(Encoding.UTF8.GetBytes(PubSub), "/resources/pubsub.yaml");
        if (withSubscription)
            builder = builder.WithResourceMapping(Encoding.UTF8.GetBytes(Subscription), "/resources/subscription.yaml");
        _sidecar = builder
            .WithCommand("./daprd", "--app-id", AppId,
                "--app-port", _appPort.ToString(CultureInfo.InvariantCulture),
                "--app-protocol", "grpc", "--app-channel-address", "host.docker.internal",
                "--dapr-http-port", HttpPort.ToString(CultureInfo.InvariantCulture),
                "--dapr-grpc-port", GrpcPort.ToString(CultureInfo.InvariantCulture),
                "--resources-path", "/resources")
            .WithPortBinding(HttpPort, true)
            .WithPortBinding(GrpcPort, true)
            // Outbound readiness deliberately does not wait for the app/subscription.
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request
                .ForPort(HttpPort).ForPath("/v1.0/healthz/outbound").ForStatusCode(HttpStatusCode.NoContent)))
            .Build();
        await _sidecar.StartAsync();
        return new DaprClientBuilder()
            .UseHttpEndpoint($"http://{_sidecar.Hostname}:{_sidecar.GetMappedPublicPort(HttpPort)}")
            .UseGrpcEndpoint($"http://{_sidecar.Hostname}:{_sidecar.GetMappedPublicPort(GrpcPort)}")
            .Build();
    }

    private async Task AssertPublishCanSucceedWithoutAQueueAsync(DaprClient client)
    {
        Assert.False(await QueueExistsAsync());
        var publisher = new DaprTopicEnqueuer<Context>(client, PubSubName, Topic,
            new FrameworkOptions { ComponentName = AppId, ServiceNamespace = "test" });
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await publisher.PublishProbeAsync(Guid.NewGuid().ToString(), deadline.Token);
        Assert.False(await QueueExistsAsync()); // even publisherConfirm cannot detect unroutable publishes
    }

    private async Task<bool> QueueExistsAsync()
    {
        var result = await _rabbit.ExecAsync(["rabbitmqctl", "-q", "list_queues", "name"]);
        Assert.Equal(0, result.ExitCode);
        return result.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Any(line => line.Trim() == Queue);
    }

    public async Task DisposeAsync()
    {
        if (_sidecar is not null)
            await _sidecar.DisposeAsync();
        await _rabbit.DisposeAsync();
        await _network.DisposeAsync();
    }

    private sealed class RecordingSendPipeline : ISendPipeline<Context>
    {
        private int _count;
        public int Count => Volatile.Read(ref _count);
        public TaskCompletionSource<byte[]> Delivered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<(StepResult<string> Result, Context Context)> Execute(ReadOnlyMemory<byte> input,
            Context context, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _count);
            Delivered.TrySetResult(input.ToArray());
            return Task.FromResult(((StepResult<string>)new StepResult<string>.Success("sent"), context));
        }
    }

    private sealed class QueueCheckedCompletion(TransactionalIntegrationDaprTests test) : FileCompletion
    {
        public bool QueueExistedAtCompletion { get; private set; }

        public override async Task CompleteAsync(IFileAdapter source, SweptFile file, FileOutcome outcome)
        {
            QueueExistedAtCompletion = await test.QueueExistsAsync();
            if (!QueueExistedAtCompletion)
                throw new InvalidOperationException("Source completion ran without the destination queue");
            await source.DeleteAsync(file.Name);
        }
    }

    private const string PubSub = $"""
        apiVersion: dapr.io/v1alpha1
        kind: Component
        metadata:
          name: {PubSubName}
        spec:
          type: pubsub.rabbitmq
          version: v1
          metadata:
            - name: connectionString
              value: amqp://guest:guest@rabbitmq:5672
            - name: durable
              value: "true"
            - name: deletedWhenUnused
              value: "false"
            - name: deliveryMode
              value: "2"
            - name: publisherConfirm
              value: "true"
        """;

    private const string Subscription = $"""
        apiVersion: dapr.io/v2alpha1
        kind: Subscription
        metadata:
          name: test-hop
        spec:
          pubsubname: {PubSubName}
          topic: {Topic}
          routes:
            default: /hop
        scopes:
          - {AppId}
        """;
}
