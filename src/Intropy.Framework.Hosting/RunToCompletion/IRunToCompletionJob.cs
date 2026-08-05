namespace Intropy.Framework.Hosting.RunToCompletion;

/// <summary>
/// A run-to-completion workload: one sweep, then exit. Scheduled externally
/// (system host locally, Kubernetes CronJob in production).
/// </summary>
/// <remarks>
/// Implementations are expected to be idempotent. A job may also abort itself by throwing
/// <see cref="OperationCanceledException"/> — e.g. when it decides no processing is needed —
/// which the runner maps to a success exit code by design.
/// </remarks>
public interface IRunToCompletionJob
{
    /// <summary>
    /// Executes the job.
    /// </summary>
    /// <param name="ct">A cancellation token that can be used to abort the job.</param>
    /// <returns>A summary of the execution. <see cref="JobRunSummary.Failed"/> greater than zero
    /// maps to a failure exit code.</returns>
    Task<JobRunSummary> ExecuteAsync(CancellationToken ct);
}
