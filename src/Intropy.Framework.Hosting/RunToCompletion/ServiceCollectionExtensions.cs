using Dapr.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.RunToCompletion;

/// <summary>
/// Extension methods for configuring run-to-completion job hosting in the service collection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <param name="services">The service collection to add services to.</param>
    extension(IServiceCollection services)
    {
        /// <summary>
        /// Adds a run-to-completion job host to the service collection.
        /// NOTE: You must register the <typeparamref name="TJob"/> implementation yourself.
        /// </summary>
        /// <typeparam name="TJob">The <see cref="IRunToCompletionJob"/> implementation to host.</typeparam>
        /// <param name="configureOptions">Action to configure the job options.</param>
        /// <returns>The service collection for chaining.</returns>
        public IServiceCollection AddRunToCompletionJob<TJob>(Action<RunToCompletionOptions> configureOptions)
            where TJob : class, IRunToCompletionJob
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(configureOptions);

            var options = new RunToCompletionOptions();
            configureOptions(options);

            if (string.IsNullOrEmpty(options.JobName))
                throw new InvalidOperationException("JobName must be configured.");

            services.AddSingleton(options);

            services.AddSingleton<IRunToCompletionJob>(sp => sp.GetRequiredService<TJob>());

            services.AddSingleton<RunToCompletionRunner>(sp =>
            {
                var daprClient = sp.GetRequiredService<DaprClient>();
                var job = sp.GetRequiredService<IRunToCompletionJob>();
                var runnerOptions = sp.GetRequiredService<RunToCompletionOptions>();
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

                return new RunToCompletionRunner(daprClient, job, runnerOptions, loggerFactory);
            });

            return services;
        }
    }
}
