using Dapr.Client;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Common;

/// <summary>
/// Shared Dapr sidecar lifecycle handling for job hosts.
/// Shutdown is bounded on purpose: a wedged sidecar must not hang the host —
/// an external scheduler would record a failed run despite a successful job.
/// </summary>
internal static class DaprSidecarManager
{
    /// <summary>
    /// Waits for the Dapr sidecar to become available, bounded by <paramref name="timeout"/>.
    /// </summary>
    public static async Task WaitAsync(
        DaprClient daprClient,
        TimeSpan timeout,
        ILogger logger,
        CancellationToken ct = default)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);

        try
        {
            await daprClient.WaitForSidecarAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Dapr sidecar did not become available within {timeout.TotalSeconds} seconds");
        }
    }

    /// <summary>
    /// Shuts down the Dapr sidecar, bounded by <paramref name="timeout"/>.
    /// Never throws: shutdown failure must not mask the job's actual outcome.
    /// </summary>
    public static async Task ShutdownAsync(DaprClient daprClient, TimeSpan timeout, ILogger logger)
    {
        try
        {
            using var timeoutCts = new CancellationTokenSource(timeout);
            await daprClient.ShutdownSidecarAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Dapr sidecar shutdown did not complete within {Timeout} seconds",
                timeout.TotalSeconds);
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "Failed to shut down Dapr sidecar");
        }
    }
}
