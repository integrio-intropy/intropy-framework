using Intropy.Framework.Blocks.BulkLoader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>
/// Registers a bulk loader: a long-running worker whose subscription declares
/// <c>bulkSubscribe</c>, so the Dapr sidecar pushes its messages in batches, and which runs each
/// batch through a bulk loader pipeline — one external call for the whole batch instead of one per
/// message.
/// </summary>
/// <remarks>
/// Whether messages arrive one at a time or in batches follows from the component's Dapr
/// <c>Subscription</c> resource, not from this registration: register a bulk loader for a
/// subscription that declares <c>bulkSubscribe</c>, and a plain loader
/// (<see cref="LoaderServiceCollectionExtensions"/>) for one that does not. A mismatch is reported
/// at the callback — the block answers, loudly, that it cannot take what it was pushed, and leaves
/// it for redelivery.
/// </remarks>
public static class BulkLoaderServiceCollectionExtensions
{
    /// <summary>
    /// Registers a bulk loader whose single pipeline handles every message on the topic, whatever
    /// its CloudEvent type.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configurePipeline">Configures the pipeline builder, given the batch's scope.</param>
    /// <param name="contextFactory">Creates the context for each message of a batch.</param>
    /// <param name="configure">Configures the subscription: pub/sub, topic, timeouts.</param>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the loader sends to the external system.</typeparam>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <returns>The service collection, for chaining.</returns>
    public static IServiceCollection AddBulkLoader<TInput, TOutput, TCtx>(
        this IServiceCollection services,
        Func<BulkLoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, BulkLoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory,
        Action<BulkLoaderOptions> configure) where TCtx : Context =>
        services.AddBulkLoader(configure, BulkLoaderRoutes.Any(configurePipeline, contextFactory));

    /// <summary>
    /// Registers a bulk loader whose single pipeline handles every message on the topic, whatever
    /// its CloudEvent type, with pipelines on the base <see cref="Context"/>; the context factory is
    /// provided for you.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configurePipeline">Configures the pipeline builder, given the batch's scope.</param>
    /// <param name="configure">Configures the subscription: pub/sub, topic, timeouts.</param>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the loader sends to the external system.</typeparam>
    /// <returns>The service collection, for chaining.</returns>
    public static IServiceCollection AddBulkLoader<TInput, TOutput>(
        this IServiceCollection services,
        Func<BulkLoaderBuilder<TInput, TOutput, Context>, IServiceProvider, BulkLoaderBuilder<TInput, TOutput, Context>> configurePipeline,
        Action<BulkLoaderOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(configure);
        return services.AddBulkLoader(configure, BulkLoaderRoutes.Any(configurePipeline,
            static (metadata, isRetry) => new Context(metadata, isRetry)));
    }

    /// <summary>
    /// Registers a bulk loader that routes each batch, by its messages' CloudEvent type, to that
    /// type's own bulk pipeline. Batches no route handles end in the broker's dead-letter queue by
    /// default (<see cref="BulkLoaderOptions.Unrouted"/>).
    /// </summary>
    /// <remarks>
    /// The sidecar evaluates the subscription's routing rules per message and groups the matches
    /// before it delivers, so one batch carries one route's messages — a type that rarely has more
    /// than one message pending simply arrives in batches of one. Each route's pipeline is built in
    /// the batch's own scope. The loader builds every route's pipeline once before subscribing, so a
    /// missing registration stops the host at startup. Caller-owned: the component identity
    /// (<c>AddIntropyFramework</c>), logging, the platform-service clients, and each route's sender
    /// and destination. Only one loader may be registered per service provider.
    /// </remarks>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configure">Configures the subscription and the unrouted policy.</param>
    /// <param name="configureRoutes">Declares the routes
    /// (<see cref="BulkLoaderRoutes.On{TInput,TOutput,TCtx}"/>,
    /// <see cref="BulkLoaderRoutes.OnAny{TInput,TOutput,TCtx}"/>).</param>
    /// <returns>The service collection, for chaining.</returns>
    public static IServiceCollection AddBulkLoader(this IServiceCollection services,
        Action<BulkLoaderOptions> configure, Action<BulkLoaderRoutes> configureRoutes)
    {
        ArgumentNullException.ThrowIfNull(configureRoutes);
        var routes = new BulkLoaderRoutes();
        configureRoutes(routes);
        return services.AddBulkLoader(configure, routes);
    }

    private static IServiceCollection AddBulkLoader(this IServiceCollection services,
        Action<BulkLoaderOptions> configure, BulkLoaderRoutes routes)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        ComponentRegistration.EnsureSingleKind(services, "loader");

        var options = new BulkLoaderOptions();
        configure(options);
        ComponentRegistration.EnsureValidCallbackPort(options.CallbackPort, nameof(BulkLoaderOptions),
            nameof(configure));

        ComponentRegistration.MarkRegistered(services, "loader");

        var table = new BulkMessageRouteTable(routes.Routes);

        // A pub/sub or topic left unset is the one the deployment names for the component, once
        // FrameworkOptions resolves; without either the loader takes whatever its Subscription
        // delivers.
        services.AddSingleton(provider =>
        {
            if (provider.GetService<FrameworkOptions>() is { } framework)
                options.UseDeploymentDefaults(framework);
            return options;
        });
        services.AddSingleton(table);

        // The host must wait for the drain: grace period, interruption and subscription teardown.
        services.Configure<HostOptions>(host =>
        {
            var required = options.ShutdownGracePeriod + TimeSpan.FromSeconds(15);
            if (host.ShutdownTimeout < required)
                host.ShutdownTimeout = required;
        });

        services.AddSingleton(provider =>
        {
            var resolved = provider.GetRequiredService<BulkLoaderOptions>();
            return new MessageConsumerSettings(resolved.PubSubName, resolved.TopicName,
                resolved.MaxBatchProcessingTime, resolved.ShutdownGracePeriod,
                AcknowledgeUnrouted: resolved.Unrouted == UnroutedPolicy.Ack);
        });
        services.AddHostedService(provider => new BulkLoaderService(
            provider.GetRequiredService<MessageConsumerSettings>(),
            table,
            provider.GetRequiredService<BulkLoaderOptions>(),
            ComponentName(provider),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<ILoggerFactory>(),
            provider.GetRequiredService<IHostApplicationLifetime>()));

        return services;
    }

    private static string ComponentName(IServiceProvider provider) =>
        provider.GetService<FrameworkOptions>()?.ComponentName ??
        throw new InvalidOperationException(
            "Bulk loader composition failed: no component identity is registered. Call AddIntropyFramework.");
}
