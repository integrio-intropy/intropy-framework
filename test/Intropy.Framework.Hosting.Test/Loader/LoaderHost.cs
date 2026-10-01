using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Hosting.Messaging.Streaming;
using Intropy.Framework.Testing.Delivery;
using Intropy.Framework.Testing.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// A generic host running a loader against fakes: the sidecar's subscription
/// (<see cref="FakeStreamingSubscriber"/>) or, under the app-callback transport, the sidecar's pushes
/// (<see cref="Callback"/>), both platform-service clients, and recording senders.
/// </summary>
public sealed class LoaderHost : IAsyncDisposable
{
    public const string PubSub = "pubsub";
    public const string Topic = "orders";
    public const string Created = "order.created";
    public const string Cancelled = "order.cancelled";

    public IHost Host { get; private set; } = null!;
    public FakeStreamingSubscriber Subscriber { get; } = new();

    /// <summary>Delivers to the loader's app callback; set when the loader runs
    /// <see cref="LoaderTransport.AppCallback"/> on an explicit <see cref="LoaderOptions.CallbackPort"/>.</summary>
    public AppCallbackDelivery? Callback { get; private set; }
    public FakeIdempotencyServiceClient Idempotency { get; } = new();
    public FakeBusinessIncidentServiceClient Incidents { get; } = new();
    public RecordingSender<OrderCreated> CreatedSender { get; } = new();
    public RecordingSender<OrderCancelled> CancelledSender { get; } = new();

    /// <summary>A loader routing <see cref="Created"/> and <see cref="Cancelled"/>.</summary>
    public static Task<LoaderHost> StartRoutingAsync(Action<LoaderOptions>? configure = null,
        Action<LoaderRoutes, LoaderHost>? extraRoutes = null, bool waitForSubscription = true,
        LoaderTransport transport = LoaderTransport.Streaming) =>
        StartAsync((services, fixture) => services.AddLoader(
            options =>
            {
                Defaults(options, transport);
                options.DeadLetterTopic = "orders.dead";
                configure?.Invoke(options);
            },
            routes =>
            {
                routes
                    .On<OrderCreated, OrderCreated, Context>(Created,
                        (builder, _) => Pipeline(builder, fixture.CreatedSender),
                        (metadata, isRetry) => new Context(metadata, isRetry))
                    .On<OrderCancelled, OrderCancelled, Context>(Cancelled,
                        (builder, _) => Pipeline(builder, fixture.CancelledSender),
                        (metadata, isRetry) => new Context(metadata, isRetry));
                extraRoutes?.Invoke(routes, fixture);
            }), waitForSubscription);

    /// <summary>A loader without routes: one pipeline for every event type.</summary>
    public static Task<LoaderHost> StartSingleAsync(Func<OrderCreated, bool>? reject = null,
        Action<LoaderOptions>? configure = null, LoaderTransport transport = LoaderTransport.Streaming) =>
        StartAsync((services, fixture) => services.AddLoader<OrderCreated, OrderCreated, Context>(
            (builder, _) => Pipeline(builder, fixture.CreatedSender, reject),
            (metadata, isRetry) => new Context(metadata, isRetry),
            options =>
            {
                Defaults(options, transport);
                configure?.Invoke(options);
            }));

    public static async Task<LoaderHost> StartAsync(Action<IServiceCollection, LoaderHost> addLoader,
        bool waitForSubscription = true)
    {
        var fixture = new LoaderHost();
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(
            new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddLogging();
        builder.Services.AddIntropyFramework(o =>
        {
            o.ComponentName = "test-loader";
            o.ServiceNamespace = "test";
        });
        builder.Services.AddSingleton<IStreamingSubscriber>(fixture.Subscriber);
        builder.Services.AddSingleton<IIdempotencyServiceClient>(fixture.Idempotency);
        builder.Services.AddSingleton<IBusinessIncidentServiceClient>(fixture.Incidents);
        addLoader(builder.Services, fixture);

        fixture.Host = builder.Build();
        await fixture.Host.StartAsync();

        var options = fixture.Host.Services.GetRequiredService<LoaderOptions>();
        if (options.Transport == LoaderTransport.AppCallback)
        {
            if (options.CallbackPort is { } port)
                fixture.Callback = new AppCallbackDelivery(port, options.PubSubName, options.TopicName);
        }
        else if (waitForSubscription)
            await fixture.Subscriber.WaitForSubscriptionAsync();
        return fixture;
    }

    public async ValueTask DisposeAsync()
    {
        await Host.StopAsync();
        Host.Dispose();
        Callback?.Dispose();
    }

    /// <summary>Delivers <paramref name="cloudEvent"/> through whichever transport the loader runs —
    /// its app callback or its streaming subscription — and returns the loader's ack.</summary>
    public Task<DeliveryAck> DeliverAsync(CloudEvent cloudEvent, bool redelivery = false) =>
        Callback is not null ? Callback.DeliverAsync(cloudEvent, redelivery) : Subscriber.DeliverAsync(cloudEvent, redelivery);

    private static void Defaults(LoaderOptions options, LoaderTransport transport = LoaderTransport.Streaming)
    {
        options.PubSubName = PubSub;
        options.TopicName = Topic;
        options.Transport = transport;
        if (transport == LoaderTransport.AppCallback)
            options.CallbackPort = AppCallbackDelivery.AvailablePort();
        options.ReconnectDelay = TimeSpan.FromMilliseconds(10);
        options.ShutdownGracePeriod = TimeSpan.FromSeconds(5);
    }

    private static LoaderBuilder<T, T, Context> Pipeline<T>(LoaderBuilder<T, T, Context> builder,
        RecordingSender<T> sender, Func<T, bool>? reject = null) =>
        builder
            .WithDeserializer(new JsonDeserializer<T>())
            .WithValidator(new TestValidator<T>(reject))
            .WithIdempotency()
            .WithTransformer(new IdentityTransformer<T>())
            .WithSender(sender)
            .WithBusinessIncidents(ctx => ctx.Metadata.GetValueOrDefault("cloudevent.subject", "unknown"),
                ctx => ctx.Metadata.GetValueOrDefault("cloudevent.subject", "unknown"));
}
