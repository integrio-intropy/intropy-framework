using Dapr.Messaging.PublishSubscribe;
using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Jobs;
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
    /// See <see cref="AddTransactionalIntegration{TCtx}"/>.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configureOptions">Action to configure the integration options.</param>
    /// <param name="configureJob">Optional runner settings: the job name (the trace activity
    /// name, default: the component name) and the sidecar timeouts.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddTransactionalIntegration(this IServiceCollection services,
        Action<TransactionalIntegrationOptions> configureOptions, Action<JobOptions>? configureJob = null) =>
        services.AddTransactionalIntegration<Context>((metadata, isRetry) => new Context(metadata, isRetry),
            configureOptions, configureJob);

    /// <summary>
    /// Adds Transactional Integration services to the service collection.
    /// This configures the whole receive side — the source sweep, and a receive pipeline that
    /// publishes each file to the integration's own topic — and the message subscription.
    /// NOTE: You must register the component identity (<c>AddIntropyFramework</c>),
    /// <c>DaprClient</c>, an ISendPipeline of <typeparamref name="TCtx"/>, and the
    /// source port (<c>AddSourcePort</c>).
    /// The lifecycle is hosted by <see cref="JobRunner"/>; resolve it and call
    /// <c>RunAsync</c>.
    /// </summary>
    /// <remarks>
    /// The receive pipeline publishes with a <see cref="DaprTopicEnqueuer{TCtx}"/> to
    /// <see cref="TransactionalIntegrationOptions.DaprPubSubName"/> /
    /// <see cref="TransactionalIntegrationOptions.DaprTopicName"/>. Both defaults are registered
    /// only when absent: register an <see cref="EnqueueStep{TCtx}"/> to replace the publisher
    /// (tests register a fake), or a whole <see cref="IReceivePipeline{TCtx}"/>.
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

        var options = new TransactionalIntegrationOptions();
        configureOptions(options);

        // Validate required options
        if (string.IsNullOrEmpty(options.DaprPubSubName))
            throw new InvalidOperationException("DaprPubSubName must be configured.");
        if (string.IsNullOrEmpty(options.DaprTopicName))
            throw new InvalidOperationException("DaprTopicName must be configured.");

        // Register options
        services.AddSingleton(options);

        // The receive side: publish each swept file to the integration's own topic.
        services.TryAddSingleton<EnqueueStep<TCtx>>(sp => new DaprTopicEnqueuer<TCtx>(
            sp.GetRequiredService<DaprClient>(), options.DaprPubSubName, options.DaprTopicName,
            sp.GetRequiredService<FrameworkOptions>()));
        services.TryAddSingleton<IReceivePipeline<TCtx>>(sp =>
        {
            var frameworkOptions = sp.GetRequiredService<FrameworkOptions>();
            return ReceivePipelineBuilder<TCtx>
                .Create($"{frameworkOptions.ComponentName}.Receive", frameworkOptions,
                    sp.GetRequiredService<ILoggerFactory>())
                .WithEnqueuer(sp.GetRequiredService<EnqueueStep<TCtx>>())
                .Build();
        });

        // Register topic subscriber
        services.AddSingleton<ITopicSubscriber>(sp =>
        {
            var pubSubClient = sp.GetRequiredService<DaprPublishSubscribeClient>();
            return new DaprTopicSubscriber(pubSubClient);
        });

        // The send side: the message processor and the subscription around it.
        services.AddSingleton(sp => new MessageProcessor<TCtx>(
            sp.GetRequiredService<ISendPipeline<TCtx>>(),
            contextFactory,
            sp.GetRequiredService<FrameworkOptions>().ComponentName,
            options.DaprTopicName,
            sp.GetRequiredService<ILoggerFactory>().CreateLogger<MessageProcessor<TCtx>>()));
        services.AddSingleton(sp => new MessageSubscriber<TCtx>(
            sp.GetRequiredService<ITopicSubscriber>(),
            sp.GetRequiredService<MessageProcessor<TCtx>>(),
            sp.GetRequiredService<TransactionalIntegrationOptions>(),
            sp.GetRequiredService<FrameworkOptions>().ComponentName,
            sp.GetRequiredService<ILoggerFactory>().CreateLogger<MessageSubscriber<TCtx>>()));

        // The receive side, and the lifecycle hosting both.
        services.AddSingleton(sp => new TransactionalIntegrationReceiver<TCtx>(sp,
            sp.GetRequiredService<FrameworkOptions>(), contextFactory, sp.GetRequiredService<ILoggerFactory>()));
        services.AddSingleton(sp => new TransactionalIntegrationJob<TCtx>(
            sp.GetRequiredService<TransactionalIntegrationReceiver<TCtx>>(),
            sp.GetRequiredService<MessageSubscriber<TCtx>>(),
            sp.GetRequiredService<ILoggerFactory>()));

        services.AddJob<TransactionalIntegrationJob<TCtx>>(configureJob);

        return services;
    }
}
