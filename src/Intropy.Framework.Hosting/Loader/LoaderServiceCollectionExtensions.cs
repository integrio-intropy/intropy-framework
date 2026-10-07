using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.Configuration;
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

    /// <summary>Registers a loader like the definition overload, with the pub/sub, topic and
    /// subscription settings supplied by a configuration section. The section's keys bind onto the
    /// definition's subscription options first — <c>PubSubName</c>, <c>TopicName</c>,
    /// <c>Unrouted</c>, <c>MaxMessageProcessingTime</c>, <c>ShutdownGracePeriod</c>,
    /// <c>CallbackPort</c> — then the caller's delegate runs, so code wins over configuration. The
    /// required pub/sub and topic are satisfied by whichever of the delegate or the section provides
    /// them; if neither does, registration fails with the existing message, so binding does not
    /// weaken validation.</summary>
    /// <remarks>
    /// The pipeline and context factory are code's to give — configuration carries settings, not
    /// wiring. A malformed value fails at registration with the member it would configure.
    /// Only one loader may be registered per service provider.
    /// </remarks>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The section holding the loader's values.</param>
    /// <param name="factory">Builds the definition the code owns — the pipeline, the context
    /// factory — and overrides anything configuration set. Leave a member unset (null) to let the
    /// configuration provide it.</param>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the loader sends to the external system.</typeparam>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <returns>The service collection, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The definition is incomplete after both
    /// configuration and the delegate have run, a configuration value is malformed, or a loader is
    /// already registered.</exception>
    public static IServiceCollection AddLoader<TInput, TOutput, TCtx>(this IServiceCollection services,
        IConfiguration configuration, Func<LoaderDefinition<TInput, TOutput, TCtx>> factory) where TCtx : Context =>
        services.AddLoader(LoaderDefinition<TInput, TOutput, TCtx>.BoundTo(configuration, factory));

    /// <summary>Binds a configuration section onto the loader's subscription options: every key the
    /// section carries sets the member it names, which the delegating members and the composed
    /// <see cref="SubscriptionOptions"/> share. Runs before the definition's own
    /// <see cref="LoaderDefinition{TInput,TOutput,TCtx}.Configure"/>, so code wins.</summary>
    internal static void ApplyConfiguration(LoaderOptions options, IConfiguration configuration)
    {
        var optionsName = nameof(LoaderOptions);
        if (ComponentConfigurationReader.String(configuration, nameof(LoaderOptions.PubSubName)) is { } pubSub)
            options.PubSubName = pubSub;
        if (ComponentConfigurationReader.String(configuration, nameof(LoaderOptions.TopicName)) is { } topic)
            options.TopicName = topic;
        if (ComponentConfigurationReader.Enum<UnroutedPolicy>(configuration, nameof(LoaderOptions.Unrouted),
                $"{optionsName}.{nameof(LoaderOptions.Unrouted)}") is { } unrouted)
            options.Unrouted = unrouted;
        if (ComponentConfigurationReader.TimeSpan(configuration, nameof(LoaderOptions.MaxMessageProcessingTime),
                $"{optionsName}.{nameof(LoaderOptions.MaxMessageProcessingTime)}") is { } processing)
            options.MaxMessageProcessingTime = processing;
        if (ComponentConfigurationReader.TimeSpan(configuration, nameof(LoaderOptions.ShutdownGracePeriod),
                $"{optionsName}.{nameof(LoaderOptions.ShutdownGracePeriod)}") is { } shutdown)
            options.ShutdownGracePeriod = shutdown;
        if (ComponentConfigurationReader.Int32(configuration, nameof(LoaderOptions.CallbackPort),
                $"{optionsName}.{nameof(LoaderOptions.CallbackPort)}") is { } port)
            options.CallbackPort = port;
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
