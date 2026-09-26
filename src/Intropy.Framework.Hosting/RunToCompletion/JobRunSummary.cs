namespace Intropy.Framework.Hosting.RunToCompletion;

/// <summary>
/// Summarizes the outcome of a run-to-completion job execution.
/// </summary>
/// <param name="Processed">The number of items the job processed.</param>
/// <param name="Failed">The number of items that failed. Greater than zero maps to a failure exit code.</param>
/// <param name="Skipped">
/// The number of items the job deliberately skipped (e.g. duplicates detected by idempotency).
/// Skipped items are not failures and do not affect the exit code.
/// </param>
public record JobRunSummary(int Processed, int Failed, int Skipped)
{
    /// <summary>
    /// A summary for a job that had nothing to process.
    /// </summary>
    public static JobRunSummary Empty => new(0, 0, 0);
}
