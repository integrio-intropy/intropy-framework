using System.Net;
using System.Net.Sockets;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Net.Client;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Hosting.Messaging.Streaming;
using Intropy.Framework.Testing.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Intropy.Framework.Hosting.Test.Loader;

public sealed record OrderState(string OrderId, string Status);

/// <summary>Looks orders up from an in-memory store; records its calls and can fail.</summary>
public sealed class OrderLookup : BatchLookupStep<OrderState>
{
    private readonly List<IReadOnlyList<string>> _calls = [];

    public IReadOnlyList<IReadOnlyList<string>> Calls
    {
        get { lock (_calls) return [.. _calls]; }
    }

    public bool Fail { get; set; }

    public override Task<IReadOnlyDictionary<string, OrderState>> LookupAsync(IReadOnlyList<string> keys,
        CancellationToken cancellationToken)
    {
        lock (_calls) _calls.Add([.. keys]);
        if (Fail)
            throw new InvalidOperationException("The order service is down");
        IReadOnlyDictionary<string, OrderState> found = keys.ToDictionary(k => k, k => new OrderState(k, "open"));
        return Task.FromResult(found);
    }
}

/// <summary>
/// A generic host running a batch loader, called the way the sidecar calls it: over gRPC on its app
/// callback port. It routes <see cref="LoaderHost.Created"/> to a batch route (filtered, keyed by
/// order, looked up) and <see cref="LoaderHost.Cancelled"/> to a message route.
/// </summary>
public sealed class BulkLoaderHost : IAsyncDisposable
{
    public IHost Host { get; private set; } = null!;
    public AppCallbackTestClient Client { get; private set; } = null!;
    public FakeStreamingSubscriber Streaming { get; } = new();
    public FakeIdempotencyServiceClient Idempotency { get; } = new();
    public FakeBusinessIncidentServiceClient Incidents { get; } = new();
    public OrderLookup Lookup { get; } = new();
    public RecordingSender<OrderState> CreatedSender { get; } = new();
    public RecordingSender<OrderCancelled> CancelledSender { get; } = new();
    private GrpcChannel? _channel;

    public static async Task<BulkLoaderHost> StartAsync(Action<LoaderOptions>? configure = null)
    {
        var fixture = new BulkLoaderHost();
        var port = FreePort();
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(
            new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddLogging();
        builder.Services.AddIntropyFramework(o =>
        {
            o.ComponentName = "test-batch-loader";
            o.ServiceNamespace = "test";
        });
        builder.Services.AddSingleton<IStreamingSubscriber>(fixture.Streaming);
        builder.Services.AddSingleton<IIdempotencyServiceClient>(fixture.Idempotency);
        builder.Services.AddSingleton<IBusinessIncidentServiceClient>(fixture.Incidents);
        builder.Services.AddLoader(
            options =>
            {
                options.PubSubName = LoaderHost.PubSub;
                options.TopicName = LoaderHost.Topic;
                options.DeadLetterTopic = "orders.dead";
                options.CallbackPort = port;
                options.MaxBatchSize = 50;
                options.MaxBatchWait = TimeSpan.FromMilliseconds(500);
                configure?.Invoke(options);
            },
            routes => routes
                .OnBatch<OrderCreated, OrderState, OrderState, Context>(LoaderHost.Created,
                    (b, _) => b
                        .WithDeserializer(new JsonDeserializer<OrderCreated>())
                        .Where(order => order.Customer != "internal")
                        .KeyedBy(order => order.OrderId)
                        .WithLookup(fixture.Lookup, chunkSize: 10)
                        .WithValidator(new TestValidator<OrderState>())
                        .WithIdempotency()
                        .WithTransformer(new IdentityTransformer<OrderState>())
                        .WithSender(fixture.CreatedSender)
                        .WithBusinessIncidents(_ => "id", _ => "subject"),
                    (metadata, isRetry) => new Context(metadata, isRetry))
                .On<OrderCancelled, OrderCancelled, Context>(LoaderHost.Cancelled,
                    (b, _) => b
                        .WithDeserializer(new JsonDeserializer<OrderCancelled>())
                        .WithValidator(new TestValidator<OrderCancelled>())
                        .WithIdempotency()
                        .WithTransformer(new IdentityTransformer<OrderCancelled>())
                        .WithSender(fixture.CancelledSender)
                        .WithBusinessIncidents(_ => "id", _ => "subject"),
                    (metadata, isRetry) => new Context(metadata, isRetry)));

        fixture.Host = builder.Build();
        await fixture.Host.StartAsync();
        fixture._channel = GrpcChannel.ForAddress($"http://127.0.0.1:{port}");
        fixture.Client = new AppCallbackTestClient(fixture._channel.CreateCallInvoker());
        return fixture;
    }

    /// <summary>Sends a bulk delivery and returns each entry's status, in order.</summary>
    public async Task<IReadOnlyList<TopicEventResponse.Types.TopicEventResponseStatus>> DeliverAsync(
        params TopicEventBulkRequestEntry[] entries)
    {
        var request = new TopicEventBulkRequest
        {
            Id = Guid.NewGuid().ToString(), PubsubName = LoaderHost.PubSub, Topic = LoaderHost.Topic
        };
        request.Entries.AddRange(entries);
        var response = await Client.OnBulkTopicEventAsync(request);
        Assert.Equal(entries.Select(e => e.EntryId), response.Statuses.Select(s => s.EntryId));
        return [.. response.Statuses.Select(s => s.Status)];
    }

    public static TopicEventBulkRequestEntry Entry(string entryId, string eventType, string subject, string data,
        int minutes = 0)
    {
        var extensions = new Struct();
        extensions.Fields["subject"] = Value.ForString(subject);
        extensions.Fields["time"] =
            Value.ForString(new DateTimeOffset(2026, 9, 29, 10, minutes, 0, TimeSpan.Zero).ToString("O"));
        return new TopicEventBulkRequestEntry
        {
            EntryId = entryId,
            ContentType = "application/cloudevents+json",
            CloudEvent = new TopicEventCERequest
            {
                Id = entryId, Source = "urn:test", Type = eventType, SpecVersion = "1.0",
                DataContentType = "application/json", Data = ByteString.CopyFromUtf8(data), Extensions = extensions
            }
        };
    }

    public async ValueTask DisposeAsync()
    {
        _channel?.Dispose();
        await Host.StopAsync();
        Host.Dispose();
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
