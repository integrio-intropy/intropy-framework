using Dapr.Messaging.PublishSubscribe;
using Dapr.Messaging.PublishSubscribe.Extensions;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Hosting.Messaging.Bulk;
using Intropy.Framework.Hosting.Messaging.Streaming;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>
/// Registers a loader: a long-running worker that consumes one topic and runs its messages through
/// loader pipelines — one message at a time through a Dapr streaming subscription (no server, no app
/// port), or in batches through Dapr bulk subscribe on a gRPC app callback when a route batches.
/// </summary>
public static class LoaderServiceCollectionExtensions
{
    /// <summary>
    /// Registers a loader whose single pipeline handles every message on the topic, whatever its
    /// CloudEvent type. Its idempotency records are keyed on the CloudEvent subject.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configurePipeline">Configures the pipeline builder, given the message's scope.</param>
    /// <param name="contextFactory">Creates the context for each message.</param>
    /// <param name="configure">Configures the subscription: pub/sub, topic, timeouts.</param>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the loader sends to the external system.</typeparam>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <returns>The service collection, for chaining.</returns>
    public static IServiceCollection AddLoader<TInput, TOutput, TCtx>(
        this IServiceCollection services,
        Func<LoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, LoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory,
        Action<LoaderOptions> configure) where TCtx : Context =>
        services.AddLoader(configure, LoaderRoutes.Any(configurePipeline, contextFactory));

    /// <summary>
    /// Registers a batch loader whose single batch pipeline handles every message on the topic. The
    /// loader receives through Dapr bulk subscribe on a gRPC app callback (see
    /// <see cref="LoaderOptions.CallbackPort"/>); its idempotency records are keyed on the entity key.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configurePipeline">Configures the batch pipeline builder, given the batch's scope.</param>
    /// <param name="contextFactory">Creates the context for each entry.</param>
    /// <param name="configure">Configures the subscription: pub/sub, topic, batch size, timeouts.</param>
    /// <typeparam name="TInput">The deserialized event.</typeparam>
    /// <typeparam name="TEnriched">What the per-entity steps run on: the looked-up state, or
    /// <typeparamref name="TInput"/> without a lookup.</typeparam>
    /// <typeparam name="TOutput">What the loader sends to the external system.</typeparam>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <returns>The service collection, for chaining.</returns>
    public static IServiceCollection AddBatchLoader<TInput, TEnriched, TOutput, TCtx>(
        this IServiceCollection services,
        Func<BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>, IServiceProvider, BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory,
        Action<LoaderOptions> configure) where TCtx : Context =>
        services.AddLoader(configure, LoaderRoutes.AnyBatch(configurePipeline, contextFactory));

    /// <summary>
    /// Registers a loader that routes each message, by its CloudEvent type, to that type's own
    /// pipeline. Messages no route handles are dropped to the dead-letter topic by default
    /// (<see cref="LoaderOptions.Unrouted"/>).
    /// </summary>
    /// <remarks>
    /// With only message routes (<see cref="LoaderRoutes.On{TInput,TOutput,TCtx}"/>) the loader
    /// consumes through a Dapr streaming subscription and serves nothing. With any batch route
    /// (<see cref="LoaderRoutes.OnBatch{TInput,TEnriched,TOutput,TCtx}"/>) it receives the whole topic in
    /// batches through Dapr bulk subscribe, served on a gRPC app callback; message routes then run their
    /// entries one at a time. Each route's pipeline is built in the message's (or batch's) own scope, so its steps may be scoped. The
    /// loader builds every route's pipeline once before subscribing, so a missing registration stops
    /// the host at startup. Caller-owned: the component identity (<c>AddIntropyFramework</c>),
    /// logging, the platform-service clients, and each route's sender and destination. Only one
    /// loader may be registered per service provider.
    /// </remarks>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configure">Configures the subscription and the unrouted policy.</param>
    /// <param name="configureRoutes">Declares the routes (<see cref="LoaderRoutes.On{TInput,TOutput,TCtx}"/>).</param>
    /// <returns>The service collection, for chaining.</returns>
    public static IServiceCollection AddLoader(this IServiceCollection services, Action<LoaderOptions> configure,
        Action<LoaderRoutes> configureRoutes)
    {
        ArgumentNullException.ThrowIfNull(configureRoutes);
        var routes = new LoaderRoutes();
        configureRoutes(routes);
        return services.AddLoader(configure, routes);
    }

    private static IServiceCollection AddLoader(this IServiceCollection services, Action<LoaderOptions> configure,
        LoaderRoutes routes)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        if (services.Any(d => d.ServiceType == typeof(Registration)))
            throw new InvalidOperationException("Only one loader may be registered per service provider.");

        var options = new LoaderOptions();
        configure(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.PubSubName, $"{nameof(LoaderOptions)}.{nameof(LoaderOptions.PubSubName)}");
        ArgumentException.ThrowIfNullOrWhiteSpace(options.TopicName, $"{nameof(LoaderOptions)}.{nameof(LoaderOptions.TopicName)}");

        var table = new LoaderRouteTable(routes.Routes);
        if (table.IsRouting && options.Unrouted == UnroutedPolicy.DeadLetter && string.IsNullOrWhiteSpace(options.DeadLetterTopic))
            throw new InvalidOperationException(
                $"The loader for topic '{options.TopicName}' dead-letters unrouted messages but has no " +
                $"{nameof(LoaderOptions.DeadLetterTopic)}; without one the sidecar would drop them silently. " +
                $"Set {nameof(LoaderOptions.DeadLetterTopic)}, or choose another {nameof(LoaderOptions.Unrouted)} policy.");

        services.AddSingleton(new Registration());
        services.AddSingleton(options);
        services.AddSingleton(table);

        // The host must wait for the drain: grace period, interruption and subscription teardown.
        services.Configure<HostOptions>(host =>
        {
            var required = options.ShutdownGracePeriod + TimeSpan.FromSeconds(15);
            if (host.ShutdownTimeout < required)
                host.ShutdownTimeout = required;
        });

        services.AddSingleton(provider => new LoaderMessageHandler(
            provider.GetRequiredService<IServiceScopeFactory>(), table, ComponentName(provider),
            provider.GetRequiredService<ILogger<LoaderMessageHandler>>()));

        if (table.HasBatchRoutes)
            AddBulkTransport(services, table, options);
        else
            AddStreamingTransport(services, table, options);

        return services;
    }

    private static void AddStreamingTransport(IServiceCollection services, LoaderRouteTable table,
        LoaderOptions options)
    {
        services.AddDaprPubSubClient();
        services.TryAddSingleton<IStreamingSubscriber>(provider =>
            new DaprStreamingSubscriber(provider.GetRequiredService<DaprPublishSubscribeClient>()));
        services.AddHostedService(provider => new LoaderService(
            provider.GetRequiredService<IStreamingSubscriber>(),
            provider.GetRequiredService<LoaderMessageHandler>(),
            table,
            options,
            ComponentName(provider),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<IHostApplicationLifetime>(),
            provider.GetRequiredService<ILogger<LoaderService>>()));
    }

    private static void AddBulkTransport(IServiceCollection services, LoaderRouteTable table, LoaderOptions options)
    {
        if (options.MaxBatchSize < 1)
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxBatchSize,
                $"{nameof(LoaderOptions)}.{nameof(LoaderOptions.MaxBatchSize)} must be at least 1.");

        services.AddSingleton(provider => new LoaderBulkDelivery(
            provider.GetRequiredService<LoaderMessageHandler>(), options, ComponentName(provider),
            provider.GetRequiredService<ILogger<LoaderBulkDelivery>>()));
        services.AddHostedService(provider => new BulkLoaderServer(
            provider.GetRequiredService<LoaderBulkDelivery>(),
            table,
            options,
            ComponentName(provider),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<ILoggerFactory>(),
            provider.GetRequiredService<IHostApplicationLifetime>()));
    }

    private static string ComponentName(IServiceProvider provider) =>
        provider.GetService<FrameworkOptions>()?.ComponentName ??
        throw new InvalidOperationException(
            "Loader composition failed: no component identity is registered. Call AddIntropyFramework.");

    private sealed class Registration;
}
