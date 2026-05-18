using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;

/// <summary>
/// Monitors message activity and determines when the system should shut down due to inactivity.
/// </summary>
/// <param name="activityTracker">The <see cref="MessageActivityTracker"/> to monitor.</param>
/// <param name="idleTimeout">The duration of inactivity before triggering shutdown.</param>
/// <param name="logger">An instance of <see cref="ILogger"/> for logging.</param>
public class IdleTimeoutMonitor(
    MessageActivityTracker activityTracker,
    TimeSpan idleTimeout,
    ILogger logger)
{
    /// <summary>
    /// Monitors for idle timeout and returns when shutdown should begin.
    /// This method blocks until the idle timeout condition is met.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the monitoring.</param>
    public async Task WaitForIdleTimeoutAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

            if (!activityTracker.ShouldShutdownDueToInactivity(idleTimeout))
                continue;

            logger.LogInformation(
                "No messages received for {TimeoutSeconds}s and no messages in progress. Shutting down.",
                idleTimeout.TotalSeconds);
            return;
        }
    }
}
