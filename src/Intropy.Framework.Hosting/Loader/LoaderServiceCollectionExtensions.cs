using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
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

        if (options.CallbackPort is < 1 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(configure), options.CallbackPort,
                $"{nameof(LoaderOptions)}.{nameof(LoaderOptions.CallbackPort)} must be a port number (1-65535).");

        var table = new LoaderRouteTable(routes.Routes);

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

        services.AddSingleton(provider => new MessageConsumer(
            new MessageConsumerSettings(options.PubSubName, options.TopicName, options.MaxMessageProcessingTime,
                options.ShutdownGracePeriod, AcknowledgeUnrouted: options.Unrouted == UnroutedPolicy.Ack),
            provider.GetRequiredService<LoaderMessageHandler>().HandleAsync,
            ComponentName(provider),
            provider.GetRequiredService<ILoggerFactory>().CreateLogger<MessageConsumer>()));
        services.AddHostedService(provider => new LoaderService(
            provider.GetRequiredService<MessageConsumer>(),
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

    private sealed class Registration;
}
