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
/// (<see cref="FakeStreamingSubscriber"/>), both platform-service clients, and recording senders.
/// </summary>
public sealed class LoaderHost : IAsyncDisposable
{
    public const string PubSub = "pubsub";
    public const string Topic = "orders";
    public const string Created = "order.created";
    public const string Cancelled = "order.cancelled";

    public IHost Host { get; private set; } = null!;
    public FakeStreamingSubscriber Subscriber { get; } = new();
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
            }));

    /// <summary>A loader without routes: one pipeline for every event type.</summary>
    public static Task<LoaderHost> StartSingleAsync(Func<OrderCreated, bool>? reject = null) =>
        StartAsync((services, fixture) => services.AddLoader<OrderCreated, OrderCreated, Context>(
            (builder, _) => Pipeline(builder, fixture.CreatedSender, reject),
            (metadata, isRetry) => new Context(metadata, isRetry),
            Defaults));

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
        if (waitForSubscription)
            await fixture.Subscriber.WaitForSubscriptionAsync();
        return fixture;
    }

    public async ValueTask DisposeAsync()
    {
        await Host.StopAsync();
        Host.Dispose();
    }

    private static void Defaults(LoaderOptions options)
    {
        options.PubSubName = PubSub;
        options.TopicName = Topic;
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
