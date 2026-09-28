namespace Intropy.Framework.Hosting.Jobs;

/// <summary>
/// What a run did with its items: a run-to-completion job's result, and the result of each part
/// of a run (a sweep of a source port, a subscription's messages) that the job adds together.
/// </summary>
/// <param name="Processed">The number of items processed.</param>
/// <param name="Failed">The number of items that failed. Greater than zero maps to a failure exit code.</param>
/// <param name="Skipped">
/// The number of items deliberately skipped (e.g. duplicates detected by idempotency).
/// Skipped items are not failures and do not affect the exit code.
/// </param>
public record RunSummary(int Processed, int Failed, int Skipped)
{
    /// <summary>
    /// A summary for a run that had nothing to process.
    /// </summary>
    public static RunSummary Empty => new(0, 0, 0);
}
