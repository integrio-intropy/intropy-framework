using CloudNative.CloudEvents;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Loader;

/// <summary>
/// Looks up the current state of a batch's entities in one call, by their keys — for events that
/// carry only an id, where every event would otherwise mean one call to the source system. The
/// batch loader calls it once per chunk of distinct keys (see
/// <see cref="BatchLoaderBuilder{TInput,TEnriched,TOutput,TCtx}.WithLookup"/>).
/// </summary>
/// <typeparam name="TEnriched">The looked-up state the rest of the pipeline runs on.</typeparam>
public abstract class BatchLookupStep<TEnriched>
{
    /// <summary>Looks up <paramref name="keys"/>.</summary>
    /// <param name="keys">Distinct entity keys, at most the configured chunk size.</param>
    /// <param name="cancellationToken">Cancels the lookup.</param>
    /// <returns>The state found for each key. A key missing from the result is handled by the
    /// configured <see cref="MissingItemPolicy"/>. Throwing fails every entry of the chunk (they are
    /// left for redelivery).</returns>
    public abstract Task<IReadOnlyDictionary<string, TEnriched>> LookupAsync(IReadOnlyList<string> keys,
        CancellationToken cancellationToken);
}

/// <summary>What the batch loader does with an entry whose key the lookup did not return.</summary>
public enum MissingItemPolicy
{
    /// <summary>A technical failure: the entry is left for redelivery (the default). Right when the
    /// source may not have the entity yet.</summary>
    Retry,

    /// <summary>A business failure: an incident is routed and the entry is consumed. Right when a
    /// missing entity needs a person to look at it.</summary>
    Incident,

    /// <summary>The entry is consumed without being sent, like a duplicate. Right when a missing
    /// entity means it was deleted and there is nothing to load.</summary>
    Skip
}

/// <summary>One entry of a batch delivered to a batch loader.</summary>
/// <param name="EntryId">The transport's id for the entry, echoed in its result.</param>
/// <param name="CloudEvent">The entry's CloudEvent.</param>
/// <param name="Context">The entry's pipeline context.</param>
public sealed record BatchEntry<TCtx>(string EntryId, CloudEvent CloudEvent, TCtx Context);

/// <summary>How one entry of a batch ended.</summary>
/// <param name="EntryId">The entry's id.</param>
/// <param name="Result">The pipeline result that decides the entry's acknowledgement. Entries
/// collapsed onto the same key share their representative's result.</param>
/// <param name="Filtered">Whether the batch loader's filter excluded the entry; its result is then
/// <c>Cancelled</c> and nothing was looked up or sent for it.</param>
public sealed record BatchEntryResult<TOutput>(
    string EntryId,
    StepResult<TOutput> Result,
    bool Filtered = false);
