using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>
/// Registers a loader: a long-running worker that consumes one topic, pushed to it by the Dapr
/// sidecar over the gRPC app callback, and runs each message through a loader pipeline.
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
    /// Registers a loader whose single pipeline handles every message on the topic, whatever its
    /// CloudEvent type, with pipelines on the base <see cref="Context"/>. See
    /// <see cref="AddLoader{TInput,TOutput,TCtx}(IServiceCollection,
    /// Func{LoaderBuilder{TInput,TOutput,TCtx}, IServiceProvider, LoaderBuilder{TInput,TOutput,TCtx}},
    /// ContextFactory{TCtx}, Action{LoaderOptions})"/>; the context factory — plain
    /// <see cref="Context"/> records for each message — is provided for you.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configurePipeline">Configures the pipeline builder, given the message's scope.</param>
    /// <param name="configure">Configures the subscription: pub/sub, topic, timeouts.</param>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the loader sends to the external system.</typeparam>
    /// <returns>The service collection, for chaining.</returns>
    public static IServiceCollection AddLoader<TInput, TOutput>(
        this IServiceCollection services,
        Func<LoaderBuilder<TInput, TOutput, Context>, IServiceProvider, LoaderBuilder<TInput, TOutput, Context>> configurePipeline,
        Action<LoaderOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(configure);
        return services.AddLoader(configure, LoaderRoutes.Any(configurePipeline,
            static (metadata, isRetry) => new Context(metadata, isRetry)));
    }

    /// <summary>
    /// Registers a loader described by a <paramref name="definition"/>: one pipeline for every
    /// message on the topic, with the pub/sub, topic and subscription settings in one object,
    /// validated at registration so a misconfigured loader fails at startup with the member that is
    /// wrong. See <see cref="LoaderDefinition{TInput,TOutput,TCtx}"/>; for a loader that routes each
    /// CloudEvent type to its own pipeline, use the routed overload
    /// (<c>AddLoader(configure, configureRoutes)</c>) instead.
    /// </summary>
    /// <remarks>
    /// The pipeline is built in each message's own scope, so steps resolved from the provider passed
    /// to the definition's <see cref="LoaderDefinition{TInput,TOutput,TCtx}.Pipeline"/> may be
    /// scoped. The loader builds the pipeline once before subscribing, so a missing registration
    /// stops the host at startup. Caller-owned: the component identity
    /// (<c>AddIntropyFramework</c>), logging, the platform-service clients, and the sender and
    /// destination. Only one loader may be registered per service provider.
    /// </remarks>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="definition">The loader description, validated at registration.</param>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the loader sends to the external system.</typeparam>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <returns>The service collection, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The definition is incomplete, or a loader is
    /// already registered.</exception>
    public static IServiceCollection AddLoader<TInput, TOutput, TCtx>(
        this IServiceCollection services, LoaderDefinition<TInput, TOutput, TCtx> definition) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(definition);
        // Validate everything before any registration: a rejected definition leaves no marker behind.
        var options = definition.Validate();
        var contextFactory = definition.ContextFactory ?? ComponentRegistration.DefaultContextFactory<TCtx>("Loader",
            nameof(LoaderDefinition<TInput, TOutput, TCtx>.ContextFactory));
        ComponentRegistration.EnsureNoOther(services, "loader");
        return AddLoaderCore(services, options, LoaderRoutes.Any(definition.Pipeline, contextFactory));
    }

    /// <summary>
    /// Registers a loader that routes each message, by its CloudEvent type, to that type's own
    /// pipeline. Messages no route handles end in the broker's dead-letter queue by default
    /// (<see cref="LoaderOptions.Unrouted"/>).
    /// </summary>
    /// <remarks>
    /// The sidecar pushes the loader's messages to a gRPC app callback the loader serves on
    /// <see cref="LoaderOptions.CallbackPort"/>; its subscription is a declarative Dapr
    /// <c>Subscription</c> resource. Each route's
    /// pipeline is built in the message's own scope, so its steps may be scoped. The
    /// loader builds every route's pipeline once before subscribing, so a missing registration stops
    /// the host at startup. Caller-owned: the component identity (<c>AddIntropyFramework</c>),
    /// logging, the platform-service clients, and each route's sender and destination. Only one
    /// loader may be registered per service provider.
    /// </remarks>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configure">Configures the subscription and the unrouted policy.</param>
    /// <param name="configureRoutes">Declares the routes (<see cref="LoaderRoutes.On{TInput,TOutput,TCtx}"/>,
    /// <see cref="LoaderRoutes.OnAny{TInput,TOutput,TCtx}"/>).</param>
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
        ComponentRegistration.EnsureSingleKind(services, "loader");

        var options = new LoaderOptions();
        configure(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.PubSubName, $"{nameof(LoaderOptions)}.{nameof(LoaderOptions.PubSubName)}");
        ArgumentException.ThrowIfNullOrWhiteSpace(options.TopicName, $"{nameof(LoaderOptions)}.{nameof(LoaderOptions.TopicName)}");
        ComponentRegistration.EnsureValidCallbackPort(options.CallbackPort, nameof(LoaderOptions), nameof(configure));

        ComponentRegistration.MarkRegistered(services, "loader");
        return AddLoaderCore(services, options, routes);
    }

    private static IServiceCollection AddLoaderCore(IServiceCollection services, LoaderOptions options,
        LoaderRoutes routes)
    {
        var table = new LoaderRouteTable(routes.Routes);

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
            provider.GetRequiredService<IServiceScopeFactory>(), table, ComponentName(provider)));

        services.AddSingleton(new MessageConsumerSettings(options.PubSubName, options.TopicName,
            options.MaxMessageProcessingTime, options.ShutdownGracePeriod,
            AcknowledgeUnrouted: options.Unrouted == UnroutedPolicy.Ack));
        services.AddHostedService(provider => new LoaderService(
            provider.GetRequiredService<MessageConsumerSettings>(),
            provider.GetRequiredService<LoaderMessageHandler>(),
            table,
            options,
            ComponentName(provider),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<ILoggerFactory>(),
            provider.GetRequiredService<IHostApplicationLifetime>()));

        return services;
    }

    private static string ComponentName(IServiceProvider provider) =>
        provider.GetService<FrameworkOptions>()?.ComponentName ??
        throw new InvalidOperationException(
            "Loader composition failed: no component identity is registered. Call AddIntropyFramework.");
}
