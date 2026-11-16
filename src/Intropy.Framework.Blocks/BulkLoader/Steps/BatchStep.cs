using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.BulkLoader.Steps;

/// <summary>
/// A pipeline position that handles every message of a batch in one call, instead of one call per
/// message. Implementations see only the entries whose chain up to this step succeeded; the bulk
/// loader passes the rest through unchanged and merges verdicts back into the original batch order —
/// implementations never partition, merge, or reorder the batch themselves.
/// </summary>
/// <typeparam name="T">Per-message input and output type.</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class BatchStep<T, TCtx> where TCtx : Context
{
    /// <summary>The step's name, used in diagnostics.</summary>
    public abstract string StepName { get; }

    /// <summary>
    /// Whether an exception <see cref="ExecuteBatchAsync"/> does not catch itself leaves every entry
    /// in the call to be tried again. The default is <see langword="true"/>, as for a
    /// <see cref="Intropy.Framework.Core.Pipeline.Abstractions.Steps.BusinessStep{TIn,TOut,TCtx}"/>.
    /// </summary>
    protected virtual bool ExceptionsAreRetriable => true;

    /// <summary>
    /// Handles every entry in one call. Must return exactly one <see cref="StepResult{T}"/> per
    /// entry, in the same order as <paramref name="entries"/>. An outcome specific to one entry maps
    /// to that entry's own result; nothing differs per entry when the whole call fails, so an
    /// uncaught exception here is turned into the same failure for every entry (see
    /// <see cref="MapException"/>) rather than tolerated as a partial result. Entries may repeat a
    /// key the external system needs only once; collapsing those into one request, and fanning the
    /// answer back out to each entry, is the implementation's job.
    /// </summary>
    /// <param name="entries">The entries to handle, each with the context it carries through the
    /// rest of the pipeline.</param>
    /// <param name="ct">A cancellation token for the call.</param>
    /// <returns>Exactly one result per entry, same order as <paramref name="entries"/>.</returns>
    public abstract Task<IReadOnlyList<StepResult<T>>> ExecuteBatchAsync(
        IReadOnlyList<(T Value, TCtx Context)> entries, CancellationToken ct);

    /// <summary>
    /// Maps an exception <see cref="ExecuteBatchAsync"/> did not catch itself into the
    /// <see cref="BusinessIncidentData"/> every entry in the call receives. A whole-batch failure is a
    /// business failure, so it reaches
    /// <see cref="Intropy.Framework.Blocks.Shared.Steps.BusinessIncidentRouteStep{T,TCtx}"/> and leaves
    /// an incident trail like any other; whether it is retried follows
    /// <see cref="ExceptionsAreRetriable"/>.
    /// </summary>
    /// <param name="exception">The exception <see cref="ExecuteBatchAsync"/> let escape.</param>
    protected virtual BusinessIncidentData MapException(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new BusinessIncidentData
        {
            Description = $"The batch step '{StepName}' failed: {exception.Message}",
            Context = new Dictionary<string, string> { ["exceptionType"] = exception.GetType().FullName ?? "?" }
        };
    }

    /// <summary>Runs <see cref="ExecuteBatchAsync"/>, validating its result count and mapping an
    /// uncaught exception into a shared failure verdict for every entry.</summary>
    internal async Task<IReadOnlyList<StepResult<T>>> ExecuteBatchAsyncInternal(
        IReadOnlyList<(T Value, TCtx Context)> entries, CancellationToken ct)
    {
        if (entries.Count == 0)
            return [];

        IReadOnlyList<StepResult<T>> results;
        try
        {
            results = await ExecuteBatchAsync(entries, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var incident = MapException(ex);
            return entries
                .Select(_ => (StepResult<T>)new StepResult<T>.BusinessFailure(incident, ExceptionsAreRetriable))
                .ToList();
        }

        if (results.Count != entries.Count)
            throw new InvalidOperationException(
                $"'{StepName}' returned {results.Count} result(s) for {entries.Count} entries; " +
                "a batch step must return exactly one verdict per entry it was given.");
        return results;
    }
}
