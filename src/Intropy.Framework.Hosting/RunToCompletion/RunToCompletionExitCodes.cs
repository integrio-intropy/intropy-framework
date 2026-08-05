namespace Intropy.Framework.Hosting.RunToCompletion;

/// <summary>
/// Process exit codes produced by <see cref="RunToCompletionRunner"/>.
/// </summary>
public static class RunToCompletionExitCodes
{
    /// <summary>
    /// The job succeeded, had nothing to do, or was cancelled. Cancellation is a
    /// success by design: the integration is idempotent and decided it does not
    /// need to process (e.g. duplicates detected), and must not be retried by the
    /// scheduler.
    /// </summary>
    public const int Success = 0;

    /// <summary>
    /// The job itself failed: it threw an unhandled exception, or reported
    /// <see cref="JobRunSummary.Failed"/> greater than zero.
    /// </summary>
    public const int JobFailure = 1;

    /// <summary>
    /// The infrastructure failed before the job could run: the Dapr sidecar did
    /// not become available within <see cref="RunToCompletionOptions.SidecarTimeoutSeconds"/>.
    /// </summary>
    public const int InfrastructureFailure = 2;
}
