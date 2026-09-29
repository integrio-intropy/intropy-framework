using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Jobs;

/// <summary>
/// Runs a host's <see cref="JobRunner"/> as the process's whole lifetime.
/// </summary>
public static class JobHostExtensions
{
    /// <summary>
    /// Starts <paramref name="host"/>, runs its <see cref="JobRunner"/>, then stops and
    /// disposes the host, and returns the exit code (see <see cref="JobExitCodes"/>).
    /// Return it from <c>Main</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Starting the host runs its hosted services, which is where OpenTelemetry's hosting
    /// integration creates the tracer, meter and logger providers. Disposing it shuts those
    /// providers down, which flushes their exporters. A job that only resolves the runner and
    /// returns exports nothing, or loses its last batch — the job span among it.
    /// </para>
    /// <para>
    /// The job is cancelled when <paramref name="ct"/> is or when the host is asked to stop (for
    /// example on SIGTERM), which the runner maps to a success exit code. Like
    /// <c>HostingAbstractionsHostExtensions.RunAsync</c>, this disposes the host.
    /// </para>
    /// </remarks>
    /// <param name="host">A host with a run-to-completion job registered (<c>AddJob</c>,
    /// <c>AddExtractor</c> or <c>AddTransactionalIntegration</c>).</param>
    /// <param name="ct">Cancels the job.</param>
    /// <returns>The runner's exit code, or <see cref="JobExitCodes.InfrastructureFailure"/>
    /// when the host could not start.</returns>
    public static async Task<int> RunToCompletionAsync(this IHost host, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(host);

        try
        {
            JobRunner runner;
            ILogger logger;
            IHostApplicationLifetime lifetime;
            try
            {
                runner = host.Services.GetRequiredService<JobRunner>();
                logger = host.Services.GetRequiredService<ILoggerFactory>()
                    .CreateLogger(typeof(JobHostExtensions).FullName!);
                lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
            }
            catch (Exception e)
            {
                // A misconfigured host must still leave through the exit-code contract (see
                // <see cref="JobExitCodes"/>), not an unhandled exception; with providers not yet
                // built there is no logger to fail into, so the console is the terminal record.
                await Console.Error.WriteLineAsync($"error: the job host is not configured to run to completion: {e.Message}");
                return JobExitCodes.InfrastructureFailure;
            }

            try
            {
                await host.StartAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                logger.LogInformation("The host was cancelled before it started");
                return JobExitCodes.Success;
            }
            catch (Exception e)
            {
                logger.LogError(e, "The host failed to start");
                return JobExitCodes.InfrastructureFailure;
            }

            try
            {
                using var jobCts = CancellationTokenSource.CreateLinkedTokenSource(ct, lifetime.ApplicationStopping);
                return await runner.RunAsync(jobCts.Token);
            }
            finally
            {
                // Bounded by the host's own ShutdownTimeout. A failed stop must not mask the job's
                // outcome; disposal below still flushes telemetry.
                try
                {
                    await host.StopAsync(CancellationToken.None);
                }
                catch (Exception e)
                {
                    logger.LogWarning(e, "The host failed to stop cleanly");
                }
            }
        }
        finally
        {
            // Shuts down the telemetry providers, flushing what the run produced.
            if (host is IAsyncDisposable asyncDisposable)
                await asyncDisposable.DisposeAsync();
            else
                host.Dispose();
        }
    }
}
