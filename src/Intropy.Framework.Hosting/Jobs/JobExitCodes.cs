namespace Intropy.Framework.Hosting.Jobs;

/// <summary>
/// Process exit codes produced by <see cref="JobRunner"/>.
/// </summary>
public static class JobExitCodes
{
    /// <summary>
    /// The job succeeded, had nothing to do, or the host cancelled it. Host
    /// cancellation is a success by design: the integration is idempotent, and the
    /// next scheduled run picks up whatever was left.
    /// </summary>
    public const int Success = 0;

    /// <summary>
    /// The job itself failed: it threw an unhandled exception, or reported
    /// <see cref="RunSummary.Failed"/> greater than zero.
    /// </summary>
    public const int JobFailure = 1;

    /// <summary>
    /// The infrastructure failed before the job could run: the Dapr sidecar did
    /// not become available within <see cref="JobOptions.SidecarTimeout"/>.
    /// </summary>
    public const int InfrastructureFailure = 2;
}
