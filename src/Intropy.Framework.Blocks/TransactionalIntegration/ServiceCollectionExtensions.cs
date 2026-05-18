using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.TransactionalIntegration;

/// <summary>
/// Extension methods for configuring Transactional Integration pipelines in the service collection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <param name="services">The service collection to add services to.</param>
    extension(IServiceCollection services)
    {
        /// <summary>
        /// Registers a send pipeline for the Transactional Integration.
        /// </summary>
        /// <typeparam name="TInput">The input type for the pipeline (after deserialization).</typeparam>
        /// <typeparam name="TOutput">The output type for the pipeline (before serialization).</typeparam>
        /// <typeparam name="TContext">The context type used throughout the pipeline.</typeparam>
        /// <param name="pipelineName">The name of the pipeline (used for logging and tracing).</param>
        /// <param name="configurePipeline">Function to configure the pipeline builder.</param>
        /// <returns>The service collection for chaining.</returns>
        public IServiceCollection AddSendPipeline<TInput, TOutput, TContext>(string pipelineName,
            Func<SendPipelineBuilder<TInput, TOutput, TContext>, IServiceProvider,
                SendPipelineBuilder<TInput, TOutput, TContext>> configurePipeline)
            where TContext : Context
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(configurePipeline);

            // Register the pipeline
            services.AddSingleton<ISendPipeline<TContext>, SendPipeline<TInput, TOutput, TContext>>(sp =>
            {
                var frameworkOptions = sp.GetRequiredService<FrameworkOptions>();
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

                var builder = SendPipelineBuilder<TInput, TOutput, TContext>
                    .Create(pipelineName, frameworkOptions, loggerFactory);

                var configuredBuilder = configurePipeline(builder, sp);
                return configuredBuilder.Build();
            });

            return services;
        }

        /// <summary>
        /// Registers a receive pipeline for the Transactional Integration.
        /// This pipeline processes source items through receive, enqueue, and complete steps.
        /// </summary>
        /// <param name="pipelineName">The name of the pipeline (used for logging and tracing).</param>
        /// <param name="configurePipeline">Function to configure the pipeline builder.</param>
        /// <returns>The service collection for chaining.</returns>
        public IServiceCollection AddReceivePipeline<TContext>(string pipelineName,
            Func<ReceivePipelineBuilder<TContext>, IServiceProvider, ReceivePipelineBuilder<TContext>> configurePipeline)
            where TContext : Context
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(configurePipeline);

            services.AddSingleton<IReceivePipeline<TContext>, ReceivePipeline<TContext>>(sp =>
            {
                var frameworkOptions = sp.GetRequiredService<FrameworkOptions>();
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

                var builder = ReceivePipelineBuilder<TContext>
                    .Create(pipelineName, frameworkOptions, loggerFactory);

                var configuredBuilder = configurePipeline(builder, sp);
                return configuredBuilder.Build();
            });

            return services;
        }
    }
}
