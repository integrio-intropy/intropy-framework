using Dapr.Client;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Jobs;

/// <summary>
/// Extension methods for configuring run-to-completion job hosting in the service collection.
/// </summary>
public static class JobServiceCollectionExtensions
{
    /// <summary>
    /// Adds a run-to-completion job host to the service collection.
    /// <typeparamref name="TJob"/> is registered as a singleton if not already registered,
    /// so an explicit registration (e.g. a pre-built instance) takes precedence.
    /// </summary>
    /// <typeparam name="TJob">The <see cref="IJob"/> implementation to host.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configureOptions">Optional job options. The job name defaults to the
    /// component name registered with <c>AddIntropyFramework</c>.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddJob<TJob>(
        this IServiceCollection services, Action<JobOptions>? configureOptions = null)
        where TJob : class, IJob
    {
        ArgumentNullException.ThrowIfNull(services);

        // Multiple runners resolve ambiguous options and sidecar lifecycles; one process is one
        // job. The guard also rejects a second job across component kinds (an extractor and a
        // transactional integration on one provider both land here).
        ComponentRegistration.EnsureNoOther(services, "job");

        var options = new JobOptions();
        configureOptions?.Invoke(options);

        // The component name is only known once the provider is built (FrameworkOptions is
        // options-bound), so the default job name is resolved then — in a new instance, never
        // by mutating the configured one: the registered options are fixed once resolved.
        services.AddSingleton(sp =>
        {
            if (!string.IsNullOrEmpty(options.JobName))
                return options;
            return sp.GetService<FrameworkOptions>()?.ComponentName is { Length: > 0 } componentName
                ? new JobOptions
                {
                    JobName = componentName,
                    SidecarTimeout = options.SidecarTimeout,
                    SidecarShutdownTimeout = options.SidecarShutdownTimeout,
                }
                : throw new InvalidOperationException(
                    "JobName is not configured and no component name is registered. Call AddIntropyFramework or set JobName.");
        });

        services.TryAddSingleton<TJob>();
        services.AddSingleton<IJob>(sp => sp.GetRequiredService<TJob>());

        services.AddSingleton<JobRunner>(sp =>
        {
            var daprClient = sp.GetRequiredService<DaprClient>();
            var job = sp.GetRequiredService<IJob>();
            var runnerOptions = sp.GetRequiredService<JobOptions>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

            return new JobRunner(daprClient, job, runnerOptions, loggerFactory);
        });

        return services;
    }
}
