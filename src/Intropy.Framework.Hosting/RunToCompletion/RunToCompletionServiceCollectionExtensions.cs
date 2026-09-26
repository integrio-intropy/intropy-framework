using Dapr.Client;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.RunToCompletion;

/// <summary>
/// Extension methods for configuring run-to-completion job hosting in the service collection.
/// </summary>
public static class RunToCompletionServiceCollectionExtensions
{
    /// <summary>
    /// Adds a run-to-completion job host to the service collection.
    /// <typeparamref name="TJob"/> is registered as a singleton if not already registered,
    /// so an explicit registration (e.g. a pre-built instance) takes precedence.
    /// </summary>
    /// <typeparam name="TJob">The <see cref="IRunToCompletionJob"/> implementation to host.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configureOptions">Optional job options. The job name defaults to the
    /// component name registered with <c>AddIntropyFramework</c>.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRunToCompletionJob<TJob>(
        this IServiceCollection services, Action<RunToCompletionOptions>? configureOptions = null)
        where TJob : class, IRunToCompletionJob
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new RunToCompletionOptions();
        configureOptions?.Invoke(options);

        // The component name is only known once the provider is built (FrameworkOptions is
        // options-bound), so the default job name is resolved then.
        services.AddSingleton(sp =>
        {
            if (string.IsNullOrEmpty(options.JobName))
                options.JobName = sp.GetService<FrameworkOptions>()?.ComponentName is { Length: > 0 } componentName
                    ? componentName
                    : throw new InvalidOperationException(
                        "JobName is not configured and no component name is registered. Call AddIntropyFramework or set JobName.");
            return options;
        });

        services.TryAddSingleton<TJob>();
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
