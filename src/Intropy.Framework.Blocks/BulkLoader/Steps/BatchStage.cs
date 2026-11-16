using System.Diagnostics;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.BulkLoader.Steps;

/// <summary>
/// Applies a <see cref="BatchStep{T,TCtx}"/> to a whole batch's current pipeline state:
/// partitions by <see cref="StepResult{T}.Success"/>/non-success — mirroring
/// <c>Pipeline.AddStep</c>'s own short-circuit, applied across a collection instead of a scalar —
/// calls the batch step once with only the success values, and merges its per-entry verdicts back
/// into the original N positions. Non-success entries pass through unchanged. Always N in, N out.
/// The call is traced as <see cref="BatchStepTracing"/> describes, against each entry's own span.
/// </summary>
internal static class BatchStage
{
    internal static async Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)[]> ApplyAsync<T, TCtx>(
        (StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)[] batch,
        BatchStep<T, TCtx> step, CancellationToken ct, IReadOnlyList<Activity?>? spans = null)
        where TCtx : Context
    {
        if (spans is not null && spans.Count != batch.Length)
            throw new ArgumentException($"Expected one span per entry ({batch.Length}), got {spans.Count}.",
                nameof(spans));

        var successIndices = new List<int>(batch.Length);
        var successEntries = new List<(T Value, TCtx Context)>(batch.Length);
        for (var i = 0; i < batch.Length; i++)
            if (batch[i].Result is StepResult<T>.Success success)
            {
                successIndices.Add(i);
                successEntries.Add((success.Value, batch[i].Context));
            }

        var output = new (StepResult<T>, TCtx, CancellationToken)[batch.Length];
        for (var i = 0; i < batch.Length; i++)
            output[i] = batch[i];
        if (successEntries.Count == 0)
            return output;

        var verdicts = await SendAsync(step, successEntries, [.. successIndices.Select(i => spans?[i])], ct);
        for (var j = 0; j < successIndices.Count; j++)
        {
            var i = successIndices[j];
            output[i] = (verdicts[j], batch[i].Context, batch[i].CancellationToken);
        }

        return output;
    }

    private static async Task<IReadOnlyList<StepResult<T>>> SendAsync<T, TCtx>(BatchStep<T, TCtx> step,
        List<(T Value, TCtx Context)> entries, Activity?[] spans, CancellationToken ct) where TCtx : Context
    {
        var start = DateTimeOffset.UtcNow;
        var batch = BatchStepTracing.StartBatch(step.StepName, spans, start);
        IReadOnlyList<StepResult<T>>? verdicts = null;
        Exception? failure = null;
        try
        {
            verdicts = await step.ExecuteBatchAsyncInternal(entries, ct);
            return verdicts;
        }
        catch (Exception e)
        {
            failure = e;
            throw;
        }
        finally
        {
            var end = DateTimeOffset.UtcNow;
            BatchStepTracing.EndBatch(batch, verdicts, failure, end);
            for (var j = 0; j < spans.Length; j++)
                BatchStepTracing.RecordMessage(step.StepName, spans[j], batch, spans.Length, verdicts?[j], failure,
                    start, end);
        }
    }
}
