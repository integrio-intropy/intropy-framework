using System.Diagnostics;
using Intropy.Framework.Blocks.BulkLoader.Steps;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.BulkLoader;

/// <summary>
/// A pipeline template for loader components that receive messages in bulk and collapse them into
/// one external call for the expensive step, instead of one call per message. It follows the same
/// steps as <see cref="Loader.Loader{TInput,TOutput,TCtx}"/> — Deserialize, Extract, Validate,
/// Idempotency check (optional), Transform, Send, Record idempotency (optional), Send receipt
/// (optional), Route business incidents — with one difference: Send is a
/// <see cref="BatchSendStep{T,TCtx}"/>, called once per batch instead of once per message.
/// </summary>
/// <remarks>
/// Conceptually this is one chain per message, split only around the batch send: every message runs
/// Deserialize through Transform on its own, in parallel up to <paramref name="preBatchDegreeOfParallelism"/>;
/// the whole batch's successes are then sent in one call; each message resumes from its own verdict
/// — success or failure, whichever the batch call gave it — through idempotency record, receipt send,
/// and business incident routing, in parallel up to <paramref name="postBatchDegreeOfParallelism"/>.
/// The business incident finalizer runs exactly once per message, at the end of its own virtual
/// chain, whether the failure that reached it came from an early step or from the batch send — same
/// as a single-message <see cref="Loader.Loader{TInput,TOutput,TCtx}"/> run.
/// </remarks>
/// <param name="pipelineName">The name of the pipeline.</param>
/// <param name="logger">Logger for tracing.</param>
/// <param name="deserializer">Step that deserializes each message's payload bytes into TInput.</param>
/// <param name="extractors">Steps that extract data from each message's input.</param>
/// <param name="validator">Step that validates each message's input.</param>
/// <param name="transformer">Step that transforms each message's TInput to TOutput.</param>
/// <param name="sender">Batch step that sends every message's output to the external system in one call.</param>
/// <param name="idempotencyChecker">Optional step that checks for duplicate processing. Skipped when null.</param>
/// <param name="idempotencyRecorder">Optional step that records successful processing. Skipped when null.</param>
/// <param name="businessIncidentRouter">Finalizer that routes business incidents.</param>
/// <param name="receiptSender">Optional step that sends a receipt after a successful load.</param>
/// <param name="preBatchDegreeOfParallelism">The maximum number of messages processed concurrently
/// before the batch send (Deserialize through Transform).</param>
/// <param name="postBatchDegreeOfParallelism">The maximum number of messages processed concurrently
/// after the batch send (idempotency record, receipt send, business incident routing).</param>
/// <typeparam name="TInput">The input type (deserialized from each message's payload).</typeparam>
/// <typeparam name="TOutput">The output type (sent to the external system).</typeparam>
/// <typeparam name="TCtx">The context type.</typeparam>
public class BulkLoader<TInput, TOutput, TCtx>(
    string pipelineName,
    ILogger logger,
    DeserializeStep<TInput, TCtx> deserializer,
    IReadOnlyList<ExtractStep<TInput, TCtx>> extractors,
    ValidateStep<TInput, TCtx> validator,
    TransformStep<TInput, TOutput, TCtx> transformer,
    BatchSendStep<TOutput, TCtx> sender,
    IdempotencyCheckStep<TInput, TCtx>? idempotencyChecker,
    IdempotencyRecordStep<TOutput, TCtx>? idempotencyRecorder,
    BusinessIncidentRouteStep<TOutput, TCtx> businessIncidentRouter,
    SendStep<TOutput, TCtx>? receiptSender = null,
    int preBatchDegreeOfParallelism = 1,
    int postBatchDegreeOfParallelism = 1)
    where TCtx : Context
{
    /// <summary>
    /// Executes the bulk loader pipeline for a whole batch of messages, each message's span a child
    /// of the current span.
    /// </summary>
    /// <param name="batch">Each message's payload bytes and its own context, in the order their
    /// results should be reported back in.</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution.</param>
    /// <returns>Exactly one result per entry of <paramref name="batch"/>, same order.</returns>
    public Task<IReadOnlyList<(StepResult<TOutput> Result, TCtx Context)>> Execute(
        IReadOnlyList<(ReadOnlyMemory<byte> Input, TCtx Context)> batch, CancellationToken ct = default) =>
        Execute(batch, parents: null, ct);

    /// <summary>
    /// Executes the bulk loader pipeline for a whole batch of messages, each message traced under
    /// its own parent.
    /// </summary>
    /// <remarks>
    /// Each message gets one pipeline span, open from its first step to its last, so the steps
    /// before the batch send and those after it sit under the same span — the shape a
    /// single-message <see cref="Loader.Loader{TInput,TOutput,TCtx}"/> run has. Given each message's
    /// own trace context as its parent, every message is traced in its own trace. The batch send
    /// is traced once, in a trace of its own linked to every message it carried, and each message
    /// gets a span for its part in it (see <see cref="BulkSendTracing"/>).
    /// </remarks>
    /// <param name="batch">Each message's payload bytes and its own context, in the order their
    /// results should be reported back in.</param>
    /// <param name="parents">The span to trace each message under, one per entry of
    /// <paramref name="batch"/>; a <see langword="default"/> entry, or no list at all, traces the
    /// message under the current span.</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution.</param>
    /// <returns>Exactly one result per entry of <paramref name="batch"/>, same order.</returns>
    public async Task<IReadOnlyList<(StepResult<TOutput> Result, TCtx Context)>> Execute(
        IReadOnlyList<(ReadOnlyMemory<byte> Input, TCtx Context)> batch, IReadOnlyList<ActivityContext>? parents,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (parents is not null && parents.Count != batch.Count)
            throw new ArgumentException($"Expected one parent per message ({batch.Count}), got {parents.Count}.",
                nameof(parents));

        var current = Activity.Current;
        var spans = new Activity?[batch.Count];
        var ended = new bool[batch.Count];
        try
        {
            // Starting a span makes it current: restore the caller's before the next one, so no
            // message's span is started under another's.
            for (var i = 0; i < batch.Count; i++)
            {
                spans[i] = PipelineTracing.StartPipelineActivity(pipelineName, parents?[i] ?? default);
                Activity.Current = current;
            }

            // Each message's half of the chain runs under its own span. Setting the current span in
            // the body is local to that message's run.
            var preBatch = await BoundedParallel.RunAsync(batch, preBatchDegreeOfParallelism,
                async (item, i, token) =>
                {
                    Activity.Current = spans[i];
                    try
                    {
                        return await Pipeline.Start(item.Input, item.Context, token)
                            .AddStep(deserializer)
                            .AddSteps(extractors)
                            .AddStep(validator)
                            .AddOptionalStep(idempotencyChecker)
                            .AddStep(transformer);
                    }
                    catch (Exception e)
                    {
                        PipelineTracing.Fail(spans[i], e, pipelineName, logger);
                        ended[i] = true;
                        throw;
                    }
                }, ct);

            var sent = await BulkSendStage.ApplyAsync(preBatch, sender, ct, spans);

            return await BoundedParallel.RunAsync(sent, postBatchDegreeOfParallelism,
                async (entry, i, token) =>
                {
                    Activity.Current = spans[i];
                    try
                    {
                        var pipeline = BulkPipeline.Resume(entry.Result, entry.Context, token)
                            .AddOptionalStep(idempotencyRecorder);
                        if (receiptSender != null)
                            pipeline = pipeline.AddStep(receiptSender);
                        var (result, context, _) = await pipeline.AddFinalizer(businessIncidentRouter);
                        PipelineTracing.Complete(spans[i], result, logger);
                        ended[i] = true;
                        return (result, context);
                    }
                    catch (Exception e)
                    {
                        PipelineTracing.Fail(spans[i], e, pipelineName, logger);
                        ended[i] = true;
                        throw;
                    }
                }, ct);
        }
        catch (Exception e)
        {
            // What ended the batch ended every message still in it: the batch send throwing, or the
            // batch being cancelled.
            for (var i = 0; i < spans.Length; i++)
                if (!ended[i])
                    spans[i]?.SetStatus(ActivityStatusCode.Error, e.Message);
            throw;
        }
        finally
        {
            foreach (var span in spans)
                span?.Dispose();
            Activity.Current = current;
        }
    }
}
