using System.Globalization;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.Loader;

/// <summary>
/// A loader pipeline that runs a whole batch of delivered entries at once. It follows these steps:
/// <list type="number">
///   <item>Deserialize (per entry): the CloudEvent data into a POCO, CloudEvent metadata into the context</item>
///   <item>Filter (per entry, optional): entries the filter excludes are consumed without further work</item>
///   <item>Key and coalesce: entries with the same entity key run once, as their latest event</item>
///   <item>Lookup (per chunk of keys, optional): the entities' current state, in one call per chunk</item>
///   <item>Per entity, with bounded concurrency: validate, idempotency check, transform, send,
///   record idempotency, route business incidents</item>
/// </list>
/// Every entry gets a result; entries collapsed onto the same key share it.
/// </summary>
/// <typeparam name="TInput">The deserialized event.</typeparam>
/// <typeparam name="TEnriched">What the per-entity steps run on: the looked-up state, or the event
/// itself when there is no lookup.</typeparam>
/// <typeparam name="TOutput">What is sent to the external system.</typeparam>
/// <typeparam name="TCtx">The context type.</typeparam>
public sealed class BatchLoader<TInput, TEnriched, TOutput, TCtx> where TCtx : Context
{
    private readonly string _pipelineName;
    private readonly ILogger _logger;
    private readonly DeserializeStep<TInput, TCtx> _deserializer;
    private readonly Func<TInput, bool>? _filter;
    private readonly Func<TInput, string> _keySelector;
    private readonly BatchLookupStep<TEnriched>? _lookup;
    private readonly int _chunkSize;
    private readonly MissingItemPolicy _missing;
    private readonly ValidateStep<TEnriched, TCtx> _validator;
    private readonly IdempotencyCheckStep<TEnriched, TCtx> _idempotencyChecker;
    private readonly TransformStep<TEnriched, TOutput, TCtx> _transformer;
    private readonly SendStep<TOutput, TCtx> _sender;
    private readonly IdempotencyRecordStep<TOutput, TCtx> _idempotencyRecorder;
    private readonly BusinessIncidentRouteStep<TOutput, TCtx> _businessIncidentRouter;
    private readonly int _maxConcurrency;

    internal BatchLoader(string pipelineName, ILogger logger, DeserializeStep<TInput, TCtx> deserializer,
        Func<TInput, bool>? filter, Func<TInput, string> keySelector, BatchLookupStep<TEnriched>? lookup,
        int chunkSize, MissingItemPolicy missing, ValidateStep<TEnriched, TCtx> validator,
        IdempotencyCheckStep<TEnriched, TCtx> idempotencyChecker, TransformStep<TEnriched, TOutput, TCtx> transformer,
        SendStep<TOutput, TCtx> sender, IdempotencyRecordStep<TOutput, TCtx> idempotencyRecorder,
        BusinessIncidentRouteStep<TOutput, TCtx> businessIncidentRouter, int maxConcurrency)
    {
        _pipelineName = pipelineName;
        _logger = logger;
        _deserializer = deserializer;
        _filter = filter;
        _keySelector = keySelector;
        _lookup = lookup;
        _chunkSize = chunkSize;
        _missing = missing;
        _validator = validator;
        _idempotencyChecker = idempotencyChecker;
        _transformer = transformer;
        _sender = sender;
        _idempotencyRecorder = idempotencyRecorder;
        _businessIncidentRouter = businessIncidentRouter;
        _maxConcurrency = maxConcurrency;
    }

    /// <summary>Runs <paramref name="entries"/> through the pipeline.</summary>
    /// <param name="entries">The delivered entries, in delivery order.</param>
    /// <param name="ct">Cancels the batch: work not yet finished ends <c>Aborted</c>.</param>
    /// <returns>One result per entry, in the order of <paramref name="entries"/>.</returns>
    public async Task<IReadOnlyList<BatchEntryResult<TOutput>>> ExecuteAsync(IReadOnlyList<BatchEntry<TCtx>> entries,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var results = new BatchEntryResult<TOutput>?[entries.Count];
        var work = new List<Work>();
        var groups = new Dictionary<string, List<Parsed>>(StringComparer.Ordinal);

        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            var (result, context, _) = await Pipeline.Start(entry.CloudEvent, entry.Context, ct).AddStep(_deserializer);
            if (result is not StepResult<TInput>.Success { Value: var input })
            {
                // Deserialization failed: the entry still runs through the per-entity steps, as a
                // failure, so the incident finalizer routes a business failure.
                work.Add(new Work(null, Convert<TInput, TEnriched>(result), context, [i]));
                continue;
            }

            if (_filter is not null && !_filter(input))
            {
                results[i] = new BatchEntryResult<TOutput>(entry.EntryId, new StepResult<TOutput>.Cancelled(), Filtered: true);
                continue;
            }

            var key = _keySelector(input);
            context.Metadata[BatchContextKeys.EntityKey] = key;
            if (!groups.TryGetValue(key, out var group))
                groups[key] = group = [];
            group.Add(new Parsed(i, input, context));
        }

        // Coalesce: an entity changed several times in one batch runs once, as its latest event.
        var representatives = groups.ToDictionary(g => g.Key, g => g.Value.MaxBy(p => (EventTime(p.Context), p.Index))!,
            StringComparer.Ordinal);
        var starts = await StartStatesAsync(representatives, ct);
        foreach (var (key, group) in groups)
            work.Add(new Work(key, starts[key], representatives[key].Context, [.. group.Select(p => p.Index)]));

        await RunAsync(work, entries, results, ct);
        return [.. results.Select(r => r!)];
    }

    private async Task<Dictionary<string, StepResult<TEnriched>>> StartStatesAsync(
        Dictionary<string, Parsed> representatives, CancellationToken ct)
    {
        var starts = new Dictionary<string, StepResult<TEnriched>>(StringComparer.Ordinal);
        if (_lookup is null)
        {
            // Without a lookup, the builder guarantees TEnriched is TInput.
            foreach (var (key, parsed) in representatives)
                starts[key] = new StepResult<TEnriched>.Success((TEnriched)(object)parsed.Input!);
            return starts;
        }

        foreach (var chunk in representatives.Keys.Chunk(_chunkSize))
        {
            IReadOnlyDictionary<string, TEnriched> found;
            try
            {
                found = await _lookup.LookupAsync(chunk, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                foreach (var key in chunk)
                    starts[key] = new StepResult<TEnriched>.Aborted();
                continue;
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "The lookup of {Count} entities failed; their entries are left for redelivery",
                    chunk.Length);
                foreach (var key in chunk)
                    starts[key] = new StepResult<TEnriched>.TechnicalFailure(
                        new TechnicalFailure("The batch lookup failed", e.Message, e, []));
                continue;
            }

            foreach (var key in chunk)
                starts[key] = found.TryGetValue(key, out var state)
                    ? new StepResult<TEnriched>.Success(state)
                    : Missing(key);
        }

        return starts;
    }

    private StepResult<TEnriched> Missing(string key) => _missing switch
    {
        MissingItemPolicy.Incident => new StepResult<TEnriched>.BusinessFailure(new BusinessIncidentData
        {
            Description = $"The entity '{key}' was not found by the lookup",
            Context = new Dictionary<string, string> { ["key"] = key }
        }),
        MissingItemPolicy.Skip => new StepResult<TEnriched>.Cancelled(),
        _ => new StepResult<TEnriched>.TechnicalFailure(
            new TechnicalFailure($"The entity '{key}' was not found by the lookup",
                "The lookup returned no state for the key", null, []))
    };

    private async Task RunAsync(List<Work> work, IReadOnlyList<BatchEntry<TCtx>> entries,
        BatchEntryResult<TOutput>?[] results, CancellationToken ct)
    {
        using var slots = new SemaphoreSlim(_maxConcurrency);
        await Task.WhenAll(work.Select(async item =>
        {
            await slots.WaitAsync(CancellationToken.None);
            try
            {
                var result = await RunEntityAsync(item, ct);
                foreach (var index in item.Members)
                    results[index] = new BatchEntryResult<TOutput>(entries[index].EntryId, result);
            }
            finally
            {
                slots.Release();
            }
        }));
    }

    private async Task<StepResult<TOutput>> RunEntityAsync(Work item, CancellationToken ct)
    {
        var (result, _) = await PipelineTracing.ExecuteWithTracing(
            () => Task.FromResult((item.Start, item.Context, ct))
                .AddStep(_validator)
                .AddStep(_idempotencyChecker)
                .AddStep(_transformer)
                .AddStep(_sender)
                .AddStep(_idempotencyRecorder)
                .AddFinalizer(_businessIncidentRouter),
            pipelineName: _pipelineName,
            logger: _logger,
            configureActivity: activity =>
            {
                if (item.Key is not null)
                    activity?.SetTag("intropy.entity.key", item.Key);
                activity?.SetTag("intropy.batch.coalesced_entries", item.Members.Count);
            },
            detachTrace: false);
        return result;
    }

    private static DateTimeOffset EventTime(TCtx context) =>
        context.Metadata.TryGetValue(CloudEventContextKeys.Time, out var time) &&
        DateTimeOffset.TryParse(time, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
            ? parsed
            : DateTimeOffset.MinValue;

    private static StepResult<TTo> Convert<TFrom, TTo>(StepResult<TFrom> result) => result switch
    {
        StepResult<TFrom>.Cancelled => new StepResult<TTo>.Cancelled(),
        StepResult<TFrom>.BusinessFailure failure => new StepResult<TTo>.BusinessFailure(failure.Value),
        StepResult<TFrom>.TechnicalFailure failure => new StepResult<TTo>.TechnicalFailure(failure.Value),
        StepResult<TFrom>.Aborted => new StepResult<TTo>.Aborted(),
        _ => throw new InvalidOperationException($"Cannot convert a {result.GetType().Name} result")
    };

    private sealed record Parsed(int Index, TInput Input, TCtx Context);

    private sealed record Work(string? Key, StepResult<TEnriched> Start, TCtx Context, IReadOnlyList<int> Members);
}
