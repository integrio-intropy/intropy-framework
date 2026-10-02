using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Testing.Delivery;
using Intropy.Framework.Testing.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// A generic host running a loader against fakes: the sidecar's pushes to the loader's app callback
/// (<see cref="Callback"/>), both platform-service clients, and recording senders.
/// </summary>
public sealed class LoaderHost : IAsyncDisposable
{
    public const string PubSub = "pubsub";
    public const string Topic = "orders";
    public const string Created = "order.created";
    public const string Cancelled = "order.cancelled";

    public IHost Host { get; private set; } = null!;
    /// <summary>Delivers to the loader's app callback, as the sidecar does; set when the loader
    /// listens on an explicit <see cref="LoaderOptions.CallbackPort"/> (the default here).</summary>
    public AppCallbackDelivery? Callback { get; private set; }
    public FakeIdempotencyServiceClient Idempotency { get; } = new();
    public FakeBusinessIncidentServiceClient Incidents { get; } = new();
    public RecordingSender<OrderCreated> CreatedSender { get; } = new();
    public RecordingSender<OrderCancelled> CancelledSender { get; } = new();

    /// <summary>A loader routing <see cref="Created"/> and <see cref="Cancelled"/>.</summary>
    public static Task<LoaderHost> StartRoutingAsync(Action<LoaderOptions>? configure = null,
        Action<LoaderRoutes, LoaderHost>? extraRoutes = null) =>
        StartAsync((services, fixture) => services.AddLoader(
            options =>
            {
                Defaults(options);
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
            }));

    /// <summary>A loader without routes: one pipeline for every event type.</summary>
    public static Task<LoaderHost> StartSingleAsync(Func<OrderCreated, bool>? reject = null,
        Action<LoaderOptions>? configure = null) =>
        StartAsync((services, fixture) => services.AddLoader<OrderCreated, OrderCreated, Context>(
            (builder, _) => Pipeline(builder, fixture.CreatedSender, reject),
            (metadata, isRetry) => new Context(metadata, isRetry),
            options =>
            {
                Defaults(options);
                configure?.Invoke(options);
            }));

    public static async Task<LoaderHost> StartAsync(Action<IServiceCollection, LoaderHost> addLoader)
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
        builder.Services.AddSingleton<IIdempotencyServiceClient>(fixture.Idempotency);
        builder.Services.AddSingleton<IBusinessIncidentServiceClient>(fixture.Incidents);
        addLoader(builder.Services, fixture);

        fixture.Host = builder.Build();
        await fixture.Host.StartAsync();

        var options = fixture.Host.Services.GetRequiredService<LoaderOptions>();
        if (options.CallbackPort is { } port)
            fixture.Callback = new AppCallbackDelivery(port, options.PubSubName, options.TopicName);
        return fixture;
    }

    public async ValueTask DisposeAsync()
    {
        await Host.StopAsync();
        Host.Dispose();
        Callback?.Dispose();
    }

    /// <summary>Delivers <paramref name="cloudEvent"/> to the loader's app callback and returns
    /// the loader's ack.</summary>
    public Task<DeliveryAck> DeliverAsync(CloudEvent cloudEvent, bool redelivery = false) =>
        Callback!.DeliverAsync(cloudEvent, redelivery);

    public Task<DeliveryAck> DeliverUnhandledAsync(CloudEvent cloudEvent) =>
        Callback!.DeliverUnhandledAsync(cloudEvent);

    /// <summary>The fixture's subscription and a free callback port, so tests can run in parallel.</summary>
    public static void Defaults(LoaderOptions options)
    {
        options.PubSubName = PubSub;
        options.TopicName = Topic;
        options.CallbackPort = AppCallbackDelivery.AvailablePort();
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
