using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.BulkLoader.Steps;

/// <summary>
/// A pipeline position that sends every message's data to an external system in one call, instead
/// of one call per message. Implementations see only the entries whose per-message phase before
/// this step succeeded; <see cref="BulkSendStage"/> handles passing the rest through unchanged and
/// merging verdicts back into the original batch order — implementations never partition, merge, or
/// reorder the batch themselves.
/// </summary>
/// <typeparam name="T">Per-message input and output type (identity, like
/// <see cref="Intropy.Framework.Blocks.Loader.Steps.SendStep{T,TCtx}"/>).</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class BatchSendStep<T, TCtx> where TCtx : Context
{
    /// <summary>The step's name, used in diagnostics.</summary>
    public virtual string StepName => "BulkSend";

    /// <summary>
    /// Sends every entry's value in one external call. Must return exactly one
    /// <see cref="StepResult{T}"/> per entry, in the same order as <paramref name="entries"/>. A
    /// rejection specific to one entry maps to that entry's own
    /// <see cref="StepResult{T}.BusinessFailure"/>; nothing differs per entry when the whole call
    /// fails, so an uncaught exception here is turned into the same failure for every entry (see
    /// <see cref="MapException"/>) rather than tolerated as a partial result.
    /// </summary>
    /// <param name="entries">The entries to send, each with the context it carries through the rest
    /// of the pipeline.</param>
    /// <param name="ct">A cancellation token for the call.</param>
    /// <returns>Exactly one result per entry, same order as <paramref name="entries"/>.</returns>
    public abstract Task<IReadOnlyList<StepResult<T>>> ExecuteBatchAsync(
        IReadOnlyList<(T Value, TCtx Context)> entries, CancellationToken ct);

    /// <summary>
    /// Maps an exception <see cref="ExecuteBatchAsync"/> did not catch itself into the
    /// <see cref="BusinessIncidentData"/> every entry in the call receives. A whole-batch failure is
    /// deliberately a business failure, not a technical one — unlike
    /// <see cref="Intropy.Framework.Core.Pipeline.Abstractions.Steps.TechnicalStep{TIn,TOut,TCtx}"/>'s
    /// own exception-handling convention — because only a business failure reaches
    /// <see cref="Intropy.Framework.Blocks.Shared.Steps.BusinessIncidentRouteStep{T,TCtx}"/>, and a
    /// batch send failure (partial or whole-batch) is required to be routed as an incident like any
    /// other business failure, not silently left as a technical retry with no incident trail.
    /// </summary>
    /// <param name="exception">The exception <see cref="ExecuteBatchAsync"/> let escape.</param>
    protected virtual BusinessIncidentData MapException(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new BusinessIncidentData
        {
            Description = $"The bulk send in step '{StepName}' failed: {exception.Message}",
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
            return entries.Select(_ => (StepResult<T>)new StepResult<T>.BusinessFailure(incident)).ToList();
        }

        if (results.Count != entries.Count)
            throw new InvalidOperationException(
                $"'{StepName}' returned {results.Count} result(s) for {entries.Count} entries; " +
                "a batch step must return exactly one verdict per entry it was given.");
        return results;
    }
}
