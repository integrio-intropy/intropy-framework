using Dapr.Client;
using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Job;

/// <summary>
/// Extension methods for configuring Transactional Integration hosting in the service collection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <param name="services">The service collection to add services to.</param>
    extension(IServiceCollection services)
    {
        /// <summary>
        /// Adds Transactional Integration services to the service collection.
        /// This configures the lifecycle including file publishing and message subscription.
        /// NOTE: You must register an instance of IReceivePipeline and ISendPipeline.
        /// </summary>
        /// <param name="configureOptions">Action to configure the integration options.</param>
        /// <returns>The service collection for chaining.</returns>
        public IServiceCollection AddTransactionalIntegration(Action<TransactionalIntegrationOptions> configureOptions)
        {
            ArgumentNullException.ThrowIfNull(services);
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

            // Register topic subscriber
            services.AddSingleton<ITopicSubscriber>(sp =>
            {
                var pubSubClient = sp.GetRequiredService<DaprPublishSubscribeClient>();
                return new DaprTopicSubscriber(pubSubClient);
            });

            // Register the lifecycle
            services.AddSingleton<TransactionalIntegrationLifecycle>(sp =>
            {
                var sourceLister = sp.GetRequiredService<ISourceLister>();
                var receivePipeline = sp.GetRequiredService<IReceivePipeline<Context>>();
                var topicSubscriber = sp.GetRequiredService<ITopicSubscriber>();
                var sendPipeline = sp.GetRequiredService<ISendPipeline<Context>>();

                var lifecycleOptions = sp.GetRequiredService<TransactionalIntegrationOptions>();
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

                return new TransactionalIntegrationLifecycle(
                    sourceLister,
                    receivePipeline,
                    topicSubscriber,
                    sendPipeline,
                    lifecycleOptions,
                    loggerFactory);
            });

            // Register the runner
            services.AddSingleton<TransactionalIntegrationRunner>(sp =>
            {
                var daprClient = sp.GetRequiredService<DaprClient>();
                var lifecycle = sp.GetRequiredService<TransactionalIntegrationLifecycle>();
                var runnerOptions = sp.GetRequiredService<TransactionalIntegrationOptions>();
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

                return new TransactionalIntegrationRunner(daprClient, lifecycle, runnerOptions, loggerFactory);
            });

            return services;
        }
    }
}
