using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// Extension methods for configuring Transactional Integration hosting in the service collection.
/// </summary>
public static class TransactionalIntegrationServiceCollectionExtensions
{
    /// <summary>
    /// Adds a Transactional Integration whose pipelines use the base <see cref="Context"/>.
    /// See <see cref="AddTransactionalIntegration{TCtx}(IServiceCollection, ContextFactory{TCtx}, Action{TransactionalIntegrationOptions}, Action{JobOptions}?)"/>.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configureOptions">Optional action to configure the integration options; without
    /// it, every option keeps its default, the internal queue included.</param>
    /// <param name="configureJob">Optional runner settings: the job name (the trace activity
    /// name, default: the component name) and the sidecar timeouts.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="InvalidOperationException">A Transactional Integration is already
    /// registered on this service provider.</exception>
    public static IServiceCollection AddTransactionalIntegration(this IServiceCollection services,
        Action<TransactionalIntegrationOptions>? configureOptions = null, Action<JobOptions>? configureJob = null) =>
        services.AddTransactionalIntegration<Context>((metadata, isRetry) => new Context(metadata, isRetry),
            configureOptions ?? (_ => { }), configureJob);

    /// <summary>
    /// Adds Transactional Integration services to the service collection.
    /// This configures the whole receive side — the source sweep, and a receive pipeline that
    /// publishes each file to the integration's own topic — and the send side, which the sidecar
    /// pushes that topic to over the gRPC app callback (<see cref="TransactionalIntegrationOptions.CallbackPort"/>);
    /// the subscription is a declarative Dapr <c>Subscription</c> resource.
    /// NOTE: You must register the component identity (<c>AddIntropyFramework</c>),
    /// <c>DaprClient</c>, an ISendPipeline of <typeparamref name="TCtx"/>, and the
    /// source port (<c>AddSourcePort</c>).
    /// The lifecycle is hosted by <see cref="JobRunner"/>; resolve it and call
    /// <c>RunAsync</c>.
    /// Only one Transactional Integration may be registered per service provider.
    /// </summary>
    /// <remarks>
    /// The receive pipeline publishes with a <see cref="DaprTopicEnqueuer{TCtx}"/> to
    /// <see cref="TransactionalIntegrationOptions.DaprPubSubName"/> /
    /// <see cref="TransactionalIntegrationOptions.DaprTopicName"/>. Both defaults are registered
    /// only when absent: register an <see cref="EnqueueStep{TCtx}"/> to replace the publisher
    /// (tests register a fake), or a whole <see cref="IReceivePipeline{TCtx}"/>. A replacement
    /// enqueuer is probed only if it implements <see cref="IInternalQueueProbe"/>; otherwise readiness is
    /// bypassed with a warning and the enqueuer owns safe handoff. A replacement receive pipeline
    /// must publish through that registered enqueuer for the probe to verify its path.
    /// For the described-by-a-definition shape
    /// (<see cref="AddTransactionalIntegration{TCtx}(IServiceCollection, TransactionalIntegrationDefinition{TCtx})"/>),
    /// the pub/sub, topic, source port and context factory are one object, validated at registration.
    /// </remarks>
    /// <typeparam name="TCtx">The context type shared by the receive and send pipelines.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="contextFactory">Creates the context for each source file and each message.</param>
    /// <param name="configureOptions">Action to configure the integration options.</param>
    /// <param name="configureJob">Optional runner settings: the job name (the trace activity
    /// name, default: the component name) and the sidecar timeouts.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddTransactionalIntegration<TCtx>(this IServiceCollection services,
        ContextFactory<TCtx> contextFactory, Action<TransactionalIntegrationOptions> configureOptions,
        Action<JobOptions>? configureJob = null) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(contextFactory);
        ArgumentNullException.ThrowIfNull(configureOptions);

        // Read only: the marker is recorded once every check that can fail has passed, so a
        // rejected registration leaves the guard open for a corrected retry.
        ComponentRegistration.EnsureSingleKind(services, "transactional integration");

        var options = new TransactionalIntegrationOptions();
        configureOptions(options);
        // Settle the composed Subscription shape against the legacy members: a value set through
        // both with different values fails here, with the members named. A name set through
        // neither defaults to the internal queue when the options are resolved.
        options.ConsolidateSubscription();

        ComponentRegistration.EnsureValidCallbackPort(options.CallbackPort,
            nameof(TransactionalIntegrationOptions), nameof(configureOptions));
        options.EnsureValidInternalQueueReadyTimeout(nameof(configureOptions));
        options.EnsureValidClaimCheckThreshold(nameof(configureOptions));

        ComponentRegistration.MarkRegistered(services, "transactional integration");
        return AddTransactionalIntegrationCore(services, options, contextFactory, configureJob);
    }

    /// <summary>
    /// Adds a Transactional Integration described by a <paramref name="definition"/>: the optional
    /// pub/sub and topic, source port, context factory and runner settings in one object, validated at
    /// registration so a misconfigured integration fails at startup with the member that is wrong.
    /// See <see cref="TransactionalIntegrationDefinition{TCtx}"/>.
    /// </summary>
    /// <remarks>
    /// This configures the whole receive side — the source sweep, and a receive pipeline that
    /// publishes each file to the integration's own topic — and the send side, which the sidecar
    /// pushes that topic to over the gRPC app callback
    /// (<see cref="TransactionalIntegrationOptions.CallbackPort"/>).
    /// NOTE: You must register the component identity (<c>AddIntropyFramework</c>),
    /// <c>DaprClient</c>, an ISendPipeline of <typeparamref name="TCtx"/>.
    /// </remarks>
    /// <typeparam name="TCtx">The context type shared by the receive and send pipelines.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="definition">The integration description, validated at registration.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="InvalidOperationException">The definition is incomplete, or a Transactional
    /// Integration is already registered.</exception>
    public static IServiceCollection AddTransactionalIntegration<TCtx>(this IServiceCollection services,
        TransactionalIntegrationDefinition<TCtx> definition) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(definition);
        var options = definition.Validate(); // before any registration: a rejected definition leaves nothing behind
        ComponentRegistration.EnsureNoOther(services, "transactional integration");
        if (definition.SourcePort is { } port)
            services.AddSourcePort(port, definition.Completion);
        return AddTransactionalIntegrationCore(services, options, definition.ResolveContextFactory(),
            definition.ConfigureJob);
    }

    /// <summary>
    /// Adds a Transactional Integration with send routes: the <paramref name="definition"/>
    /// describes the integration (the optional pub/sub and topic, the context factory, the options
    /// and the runner settings), <paramref name="configureSources"/> declares the source ports it
    /// sweeps, and <paramref name="configureRoutes"/> which send pipeline each port's messages go
    /// through. Validated at registration, so a misconfigured integration fails at startup.
    /// </summary>
    /// <remarks>
    /// Every port's files go through the one receive side to the integration's queue. The framework
    /// records each file's port on its message, so a step cannot misroute it. The ports are swept one
    /// after another, in declaration order; a port that fails does not stop the others, but the run
    /// still fails.
    /// NOTE: You must register the component identity (<c>AddIntropyFramework</c>),
    /// <c>DaprClient</c>, and each port's file adapter, keyed by the port name. Do not register an
    /// <c>ISendPipeline</c> or call <c>AddSourcePort</c>: the sources and routes declare both.
    /// </remarks>
    /// <typeparam name="TCtx">The context type shared by the receive and send pipelines.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="definition">The integration description, validated at registration. Its
    /// <see cref="TransactionalIntegrationDefinition{TCtx}.SourcePort"/> and
    /// <see cref="TransactionalIntegrationDefinition{TCtx}.Completion"/> must be unset: the sources
    /// declare them.</param>
    /// <param name="configureSources">Declares the source ports and their completions.</param>
    /// <param name="configureRoutes">Declares the send routes.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="InvalidOperationException">The definition is incomplete or sets a source
    /// port; no source or route is declared; a route for every file is combined with other routes;
    /// a route names an undeclared port, a port is routed twice or not at all; a source port or a
    /// Transactional Integration is already registered.</exception>
    public static IServiceCollection AddTransactionalIntegration<TCtx>(this IServiceCollection services,
        TransactionalIntegrationDefinition<TCtx> definition, Action<FileSources> configureSources,
        Action<TransactionalIntegrationRoutes<TCtx>> configureRoutes) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(configureSources);
        ArgumentNullException.ThrowIfNull(configureRoutes);
        // Everything that can fail runs before any registration: a rejected definition leaves nothing behind.
        var options = definition.Validate();
        if (definition.SourcePort is not null || definition.Completion is not null)
            throw new InvalidOperationException(
                "Transactional Integration composition failed: SourcePort/Completion — an integration with routes " +
                "declares its source ports and their completions with its sources; leave the definition's unset.");
        var sources = new FileSources();
        configureSources(sources);
        var routes = new TransactionalIntegrationRoutes<TCtx>();
        configureRoutes(routes);
        sources.EnsureRoutedBy([.. routes.Matches], "Transactional Integration");
        var contextFactory = definition.ResolveContextFactory();
        var table = new MessageRouteTable(routes.Build(contextFactory));
        ComponentRegistration.EnsureSingleKind(services, "transactional integration");
        SourcePortServiceCollectionExtensions.AddSourcePorts(services, sources.Ports);
        ComponentRegistration.MarkRegistered(services, "transactional integration");

        return AddTransactionalIntegrationCore(services, options, contextFactory, definition.ConfigureJob, table);
    }

    /// <summary>
    /// Adds a Transactional Integration with send routes, with the integration's values bound from
    /// <paramref name="configuration"/>. See
    /// <see cref="AddTransactionalIntegration{TCtx}(IServiceCollection, TransactionalIntegrationDefinition{TCtx}, Action{FileSources}, Action{TransactionalIntegrationRoutes{TCtx}})"/>
    /// for the sources and routes, and
    /// <see cref="AddTransactionalIntegration{TCtx}(IServiceCollection, IConfiguration, Func{TransactionalIntegrationDefinition{TCtx}})"/>
    /// for how the configuration and the factory's definition combine.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The section holding the integration's values.</param>
    /// <param name="factory">Builds the definition the code owns; leave a member unset (null) to let
    /// the configuration provide it. Its source port and completion must be unset.</param>
    /// <param name="configureSources">Declares the source ports and their completions.</param>
    /// <param name="configureRoutes">Declares the send routes.</param>
    /// <typeparam name="TCtx">The context type shared by the receive and send pipelines.</typeparam>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="InvalidOperationException">As for the definition overload, or a
    /// configuration value is malformed.</exception>
    public static IServiceCollection AddTransactionalIntegration<TCtx>(this IServiceCollection services,
        IConfiguration configuration, Func<TransactionalIntegrationDefinition<TCtx>> factory,
        Action<FileSources> configureSources, Action<TransactionalIntegrationRoutes<TCtx>> configureRoutes)
        where TCtx : Context =>
        services.AddTransactionalIntegration(TransactionalIntegrationDefinition<TCtx>.BoundTo(configuration, factory),
            configureSources, configureRoutes);

    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The section holding the integration's values: the pub/sub and
    /// topic (<c>DaprPubSubName</c>/<c>DaprTopicName</c>, or <c>PubSubName</c>/<c>TopicName</c>),
    /// the timeouts (<c>MaxMessageProcessingTime</c>, <c>IdleTimeout</c>,
    /// <c>PostIdleGracePeriod</c>) and the <c>CallbackPort</c>. Each value only sets what the
    /// definition does not; a malformed value fails here, with the member it would configure.</param>
    /// <param name="factory">Builds the definition the code owns — the context factory, the source
    /// port — and overrides anything configuration set. Leave a member unset (null) to let the
    /// configuration provide it.</param>
    /// <typeparam name="TCtx">The context type shared by the receive and send pipelines.</typeparam>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="InvalidOperationException">The definition is incomplete after both
    /// configuration and the factory have run, a configuration value is malformed, or a
    /// Transactional Integration is already registered.</exception>
    public static IServiceCollection AddTransactionalIntegration<TCtx>(this IServiceCollection services,
        IConfiguration configuration, Func<TransactionalIntegrationDefinition<TCtx>> factory)
        where TCtx : Context =>
        services.AddTransactionalIntegration(
            TransactionalIntegrationDefinition<TCtx>.BoundTo(configuration, factory));

    /// <summary>Why no send route handles a message: the source port the receive side stamped it
    /// with, or (none) when it was published without the stamp — it names no port, rather than a
    /// port with an empty name.</summary>
    internal static string DescribeUnroutedSend(IncomingMessage message) =>
        message.GetExtension(CloudEventExtensionKeys.SourcePort) is { Length: > 0 } port
            ? $"No send route handles source port '{port}'"
            : "No send route handles source port '(none)': it names no port";

    /// <summary>Binds a configuration section onto the integration options: every key the section
    /// carries sets the member it names — the legacy <c>DaprPubSubName</c>/<c>DaprTopicName</c>
    /// spellings and the composed <see cref="TransactionalIntegrationOptions.Subscription"/>
    /// shape, whose delegating members are the same values. Runs before the definition's own
    /// <see cref="TransactionalIntegrationDefinition{TCtx}.Configure"/>, so code wins.</summary>
    internal static void ApplyConfiguration(TransactionalIntegrationOptions options, IConfiguration configuration)
    {
        if (ComponentConfigurationReader.String(configuration, "DaprPubSubName") is { } pubSub)
            options.DaprPubSubName = pubSub;
        if (ComponentConfigurationReader.String(configuration, "DaprTopicName") is { } topic)
            options.DaprTopicName = topic;
        // The composed shape: an additional way to set the pub/sub and topic, and the shape the
        // delegating members store through.
        if (ComponentConfigurationReader.String(configuration, "PubSubName") is { } composedPubSub)
            options.Subscription.PubSubName = composedPubSub;
        if (ComponentConfigurationReader.String(configuration, "TopicName") is { } composedTopic)
            options.Subscription.TopicName = composedTopic;
        if (ComponentConfigurationReader.TimeSpan(configuration, nameof(TransactionalIntegrationOptions.MaxMessageProcessingTime),
                $"{nameof(TransactionalIntegrationOptions)}.{nameof(TransactionalIntegrationOptions.MaxMessageProcessingTime)}") is { } processing)
            options.MaxMessageProcessingTime = processing;
        if (ComponentConfigurationReader.TimeSpan(configuration, nameof(TransactionalIntegrationOptions.IdleTimeout),
                $"{nameof(TransactionalIntegrationOptions)}.{nameof(TransactionalIntegrationOptions.IdleTimeout)}") is { } idle)
            options.IdleTimeout = idle;
        if (ComponentConfigurationReader.TimeSpan(configuration, nameof(TransactionalIntegrationOptions.PostIdleGracePeriod),
                $"{nameof(TransactionalIntegrationOptions)}.{nameof(TransactionalIntegrationOptions.PostIdleGracePeriod)}") is { } grace)
            options.PostIdleGracePeriod = grace;
        if (ComponentConfigurationReader.TimeSpan(configuration, nameof(TransactionalIntegrationOptions.InternalQueueReadyTimeout),
                $"{nameof(TransactionalIntegrationOptions)}.{nameof(TransactionalIntegrationOptions.InternalQueueReadyTimeout)}") is { } internalQueueReady)
            options.InternalQueueReadyTimeout = internalQueueReady;
        if (ComponentConfigurationReader.Int32(configuration, nameof(TransactionalIntegrationOptions.CallbackPort),
                $"{nameof(TransactionalIntegrationOptions)}.{nameof(TransactionalIntegrationOptions.CallbackPort)}") is { } port)
            options.CallbackPort = port;
        if (ComponentConfigurationReader.Enum<ClaimCheckMode>(configuration, nameof(TransactionalIntegrationOptions.ClaimCheckMode),
                $"{nameof(TransactionalIntegrationOptions)}.{nameof(TransactionalIntegrationOptions.ClaimCheckMode)}") is { } claimCheckMode)
            options.ClaimCheckMode = claimCheckMode;
        if (ComponentConfigurationReader.Int32(configuration, nameof(TransactionalIntegrationOptions.ClaimCheckThresholdBytes),
                $"{nameof(TransactionalIntegrationOptions)}.{nameof(TransactionalIntegrationOptions.ClaimCheckThresholdBytes)}") is { } claimCheckThreshold)
            options.ClaimCheckThresholdBytes = claimCheckThreshold;
    }

    // routes: the send routes, or null for the one caller-registered send pipeline for every file.
    private static IServiceCollection AddTransactionalIntegrationCore<TCtx>(IServiceCollection services,
        TransactionalIntegrationOptions options, ContextFactory<TCtx> contextFactory,
        Action<JobOptions>? configureJob, MessageRouteTable? routes = null) where TCtx : Context
    {
        // Register options. A pub/sub or topic left unset is the internal queue the system topology
        // generates for the component, named after its identity, which resolves from DI.
        services.AddSingleton(sp =>
        {
            if (string.IsNullOrEmpty(options.DaprPubSubName) || string.IsNullOrEmpty(options.DaprTopicName))
                options.UseInternalQueueDefaults(sp.GetRequiredService<FrameworkOptions>());
            return options;
        });

        // The receive side: publish each swept file to the integration's own topic.
        services.TryAddSingleton<EnqueueStep<TCtx>>(sp =>
        {
            var resolved = sp.GetRequiredService<TransactionalIntegrationOptions>();
            return new DaprTopicEnqueuer<TCtx>(sp.GetRequiredService<DaprClient>(), resolved.DaprPubSubName,
                resolved.DaprTopicName, sp.GetRequiredService<FrameworkOptions>());
        });
        services.TryAddSingleton<IReceivePipeline<TCtx>>(sp =>
        {
            var frameworkOptions = sp.GetRequiredService<FrameworkOptions>();
            var builder = ReceivePipelineBuilder<TCtx>
                .Create($"{frameworkOptions.ComponentName}.Receive", frameworkOptions,
                    sp.GetRequiredService<ILoggerFactory>())
                .WithEnqueuer(sp.GetRequiredService<EnqueueStep<TCtx>>());
            var claimStore = sp.GetKeyedService<IFileAdapter>(ClaimStoreServiceCollectionExtensions.ClaimStoreKey);
            if (claimStore is not null)
            {
                var resolved = sp.GetRequiredService<TransactionalIntegrationOptions>();
                if (resolved.ClaimCheckPolicy() is { } policy)
                    builder = builder.WithClaimCheck(claimStore, policy);
                else
                    sp.GetRequiredService<ILoggerFactory>()
                        .CreateLogger(typeof(ClaimStoreServiceCollectionExtensions))
                        .LogInformation(
                            "Claim store registered; claim check mode {Mode}: resolving pointers, writing none",
                            resolved.ClaimCheckMode);
            }
            return builder.Build();
        });

        // The send side: the message processor, and the callback the sidecar pushes the queue to.
        // Without routes, the caller-registered send pipeline handles every message. The routes'
        // pipelines are built once here, so a missing registration fails at startup, not per message.
        services.AddSingleton(sp =>
        {
            var componentName = sp.GetRequiredService<FrameworkOptions>().ComponentName;
            var scopes = sp.GetRequiredService<IServiceScopeFactory>();
            var table = routes ?? new MessageRouteTable([new RegisteredSendRoute<TCtx>(contextFactory)]);
            table.Verify(scopes, componentName);
            return new SendSideRun(
                new RoutingMessageHandler(scopes, table, componentName,
                    DescribeUnroutedSend,
                    // The internal hop's Subscription has no rules: every message takes its default route.
                    defaultPathIsUnrouted: false),
                sp.GetRequiredService<TransactionalIntegrationOptions>(),
                componentName,
                sp.GetRequiredService<ILoggerFactory>());
        });

        // The receive side, and the lifecycle hosting both.
        services.AddSingleton(sp => new TransactionalIntegrationReceiver<TCtx>(sp,
            sp.GetRequiredService<FrameworkOptions>(), contextFactory, sp.GetRequiredService<ILoggerFactory>()));
        // The internal queue is probed through the enqueuer that publishes the files, so a replaced enqueuer
        // (a test fake, another transport) that cannot be probed sweeps without the gate.
        services.AddSingleton(sp =>
        {
            // The receive pipeline is built once here for the same reason the send routes are: so a
            // configuration it cannot be built from — a claim store with no threshold, say — fails
            // alongside them, rather than once the sidecar is up and the queue has been probed. The
            // sweep validates it again per port before listing a file, which is what keeps a bad
            // configuration from touching one; this only moves the report earlier.
            _ = sp.GetRequiredService<IReceivePipeline<TCtx>>();
            return new TransactionalIntegrationJob<TCtx>(
                sp.GetRequiredService<TransactionalIntegrationReceiver<TCtx>>(),
                sp.GetRequiredService<SendSideRun>(),
                sp.GetRequiredService<ILoggerFactory>(),
                InternalQueueReadinessGateFor(sp.GetRequiredService<EnqueueStep<TCtx>>(),
                    sp.GetRequiredService<TransactionalIntegrationOptions>(), sp.GetRequiredService<ILoggerFactory>()));
        });

        services.AddJob<TransactionalIntegrationJob<TCtx>>(configureJob);

        return services;
    }

    private static InternalQueueReadinessGate? InternalQueueReadinessGateFor<TCtx>(EnqueueStep<TCtx> enqueuer,
        TransactionalIntegrationOptions options, ILoggerFactory loggerFactory) where TCtx : Context =>
        enqueuer is IInternalQueueProbe probe
            ? new InternalQueueReadinessGate(probe, options.InternalQueueReadyTimeout, options.InternalQueueProbeInterval,
                loggerFactory.CreateLogger<InternalQueueReadinessGate>())
            : null;
}
