using Dapr.Messaging.PublishSubscribe;
using Dapr.Messaging.PublishSubscribe.Extensions;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Hosting.Messaging.Streaming;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>
/// Registers a loader: a long-running worker that consumes one topic through a Dapr streaming
/// subscription (no HTTP server, no app port) and runs each message through a loader pipeline.
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
    /// pipeline. Messages no route handles are dropped to the dead-letter topic by default
    /// (<see cref="LoaderOptions.Unrouted"/>).
    /// </summary>
    /// <remarks>
    /// Each route's pipeline is built in the message's own scope, so its steps may be scoped. The
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

        services.AddDaprPubSubClient();
        services.TryAddSingleton<IStreamingSubscriber>(provider =>
            new DaprStreamingSubscriber(provider.GetRequiredService<DaprPublishSubscribeClient>()));

        services.AddSingleton(provider => new LoaderMessageHandler(
            provider.GetRequiredService<IServiceScopeFactory>(), table, ComponentName(provider),
            provider.GetRequiredService<ILogger<LoaderMessageHandler>>()));
        services.AddHostedService(provider => new LoaderService(
            provider.GetRequiredService<IStreamingSubscriber>(),
            provider.GetRequiredService<LoaderMessageHandler>(),
            table,
            options,
            ComponentName(provider),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<IHostApplicationLifetime>(),
            provider.GetRequiredService<ILogger<LoaderService>>()));

        return services;
    }

    private static string ComponentName(IServiceProvider provider) =>
        provider.GetService<FrameworkOptions>()?.ComponentName ??
        throw new InvalidOperationException(
            "Loader composition failed: no component identity is registered. Call AddIntropyFramework.");

    private sealed class Registration;
}
