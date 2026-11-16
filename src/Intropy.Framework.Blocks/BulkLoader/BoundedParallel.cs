namespace Intropy.Framework.Blocks.BulkLoader;

/// <summary>
/// Runs one async function over every item of a batch with a bounded degree of parallelism,
/// preserving input order in the output regardless of completion order. A degree of 1 runs the
/// items sequentially.
/// </summary>
internal static class BoundedParallel
{
    /// <summary>
    /// Runs <paramref name="body"/> once per item of <paramref name="items"/>, with at most
    /// <paramref name="maxDegreeOfParallelism"/> running at a time.
    /// </summary>
    /// <param name="items">The items to process. Each is given to <paramref name="body"/> along
    /// with its position in the list.</param>
    /// <param name="maxDegreeOfParallelism">The maximum number of items processed concurrently.
    /// Values below 1 are treated as 1 (sequential).</param>
    /// <param name="body">The async function to run per item.</param>
    /// <param name="ct">A cancellation token observed between items.</param>
    internal static async Task<TOut[]> RunAsync<TIn, TOut>(
        IReadOnlyList<TIn> items, int maxDegreeOfParallelism,
        Func<TIn, int, CancellationToken, Task<TOut>> body, CancellationToken ct)
    {
        var results = new TOut[items.Count];
        if (items.Count == 0)
            return results;

        await Parallel.ForEachAsync(
            Enumerable.Range(0, items.Count),
            new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, maxDegreeOfParallelism), CancellationToken = ct },
            async (i, token) => results[i] = await body(items[i], i, token));

        return results;
    }
}
