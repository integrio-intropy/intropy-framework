using System.Diagnostics;
using Intropy.Framework.Blocks.BulkLoader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.BulkLoader;

/// <summary>
/// A pipeline template for loader components that receive messages in bulk. It follows the same
/// steps as <see cref="Loader.Loader{TInput,TOutput,TCtx}"/> — Deserialize, Extract, Validate,
/// Idempotency check (optional), Transform, Send, Record idempotency (optional), Send receipt
/// (optional), Route business incidents — and lets each extractor and the sender run either once
/// per message or once for the whole batch (<see cref="BatchExtractStep{TInput,TCtx}"/>,
/// <see cref="BatchSendStep{T,TCtx}"/>), so an expensive call can be made once instead of once per
/// message. Build one with <see cref="BulkLoaderBuilder{TInput,TOutput,TCtx}"/>.
/// </summary>
/// <remarks>
/// Conceptually this is one chain per message, split at each batch step. The steps between two batch
/// steps run per message, in parallel up to the configured degree; each batch step is then called
/// once with the messages still succeeding, and each message resumes from its own verdict — success
/// or failure, whichever the batch call gave it. A message that fails before a batch step is left out
/// of it and passes through unchanged. The business incident finalizer runs exactly once per message,
/// at the end of its own chain, whichever step the failure that reached it came from — same as a
/// single-message <see cref="Loader.Loader{TInput,TOutput,TCtx}"/> run.
/// </remarks>
/// <typeparam name="TInput">The input type (deserialized from each message's payload).</typeparam>
/// <typeparam name="TOutput">The output type (sent to the external system).</typeparam>
/// <typeparam name="TCtx">The context type.</typeparam>
public class BulkLoader<TInput, TOutput, TCtx> where TCtx : Context
{
    private readonly string _pipelineName;
    private readonly ILogger _logger;
    private readonly DeserializeStep<TInput, TCtx> _deserializer;
    private readonly IReadOnlyList<ExtractStep<TInput, TCtx>> _leadingExtractors;
    private readonly IReadOnlyList<(BatchExtractStep<TInput, TCtx> Batch, IReadOnlyList<ExtractStep<TInput, TCtx>> Then)> _batchExtractors;
    private readonly ValidateStep<TInput, TCtx> _validator;
    private readonly TransformStep<TInput, TOutput, TCtx> _transformer;
    private readonly SendStep<TOutput, TCtx>? _sender;
    private readonly BatchSendStep<TOutput, TCtx>? _batchSender;
    private readonly IdempotencyCheckStep<TInput, TCtx>? _idempotencyChecker;
    private readonly IdempotencyRecordStep<TOutput, TCtx>? _idempotencyRecorder;
    private readonly BusinessIncidentRouteStep<TOutput, TCtx> _businessIncidentRouter;
    private readonly SendReceiptStep<TOutput, TCtx>? _receiptSender;
    private readonly int _degreeOfParallelism;

    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="logger">Logger for tracing.</param>
    /// <param name="deserializer">Step that deserializes each message's payload bytes into TInput.</param>
    /// <param name="extractors">Steps that extract data from each message's input, in order, each
    /// either per message or for the whole batch.</param>
    /// <param name="validator">Step that validates each message's input.</param>
    /// <param name="transformer">Step that transforms each message's TInput to TOutput.</param>
    /// <param name="sender">Step that sends each message's output on its own; null when
    /// <paramref name="batchSender"/> is given.</param>
    /// <param name="batchSender">Step that sends every message's output in one call; null when
    /// <paramref name="sender"/> is given.</param>
    /// <param name="idempotencyChecker">Optional step that checks for duplicate processing.</param>
    /// <param name="idempotencyRecorder">Optional step that records successful processing.</param>
    /// <param name="businessIncidentRouter">Finalizer that routes business incidents.</param>
    /// <param name="receiptSender">Optional step that sends a receipt after a successful load.</param>
    /// <param name="degreeOfParallelism">The maximum number of messages the per-message steps
    /// process concurrently.</param>
    internal BulkLoader(
        string pipelineName,
        ILogger logger,
        DeserializeStep<TInput, TCtx> deserializer,
        IReadOnlyList<(ExtractStep<TInput, TCtx>? PerMessage, BatchExtractStep<TInput, TCtx>? Batch)> extractors,
        ValidateStep<TInput, TCtx> validator,
        TransformStep<TInput, TOutput, TCtx> transformer,
        SendStep<TOutput, TCtx>? sender,
        BatchSendStep<TOutput, TCtx>? batchSender,
        IdempotencyCheckStep<TInput, TCtx>? idempotencyChecker,
        IdempotencyRecordStep<TOutput, TCtx>? idempotencyRecorder,
        BusinessIncidentRouteStep<TOutput, TCtx> businessIncidentRouter,
        SendReceiptStep<TOutput, TCtx>? receiptSender,
        int degreeOfParallelism)
    {
        if ((sender is null) == (batchSender is null))
            throw new ArgumentException("Exactly one of a per-message sender and a batch sender must be given.");

        _pipelineName = pipelineName;
        _logger = logger;
        _deserializer = deserializer;
        _validator = validator;
        _transformer = transformer;
        _sender = sender;
        _batchSender = batchSender;
        _idempotencyChecker = idempotencyChecker;
        _idempotencyRecorder = idempotencyRecorder;
        _businessIncidentRouter = businessIncidentRouter;
        _receiptSender = receiptSender;
        _degreeOfParallelism = degreeOfParallelism;

        // Group the extractors at each batch extractor: the per-message ones before the first run in
        // the same pass as Deserialize, and those after each batch extractor in the pass it resumes.
        var leading = new List<ExtractStep<TInput, TCtx>>();
        var batches = new List<(BatchExtractStep<TInput, TCtx>, IReadOnlyList<ExtractStep<TInput, TCtx>>)>();
        var group = leading;
        foreach (var (perMessage, batch) in extractors)
        {
            if (batch is not null)
            {
                group = [];
                batches.Add((batch, group));
            }
            else if (perMessage is not null)
            {
                group.Add(perMessage);
            }
        }

        _leadingExtractors = leading;
        _batchExtractors = batches;
    }

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
    /// Each message gets one pipeline span, open from its first step to its last, so the steps on
    /// either side of a batch step sit under the same span — the shape a single-message
    /// <see cref="Loader.Loader{TInput,TOutput,TCtx}"/> run has. Given each message's own trace
    /// context as its parent, every message is traced in its own trace. Each batch step is traced
    /// once, in a trace of its own linked to every message it carried, and each message gets a span
    /// for its part in it (see <see cref="BatchStepTracing"/>).
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
                spans[i] = PipelineTracing.StartPipelineActivity(_pipelineName, parents?[i] ?? default);
                Activity.Current = current;
            }

            var extracted = await PerMessageAsync(batch, spans, ended, (item, _, token) =>
                Pipeline.Start(item.Input, item.Context, token)
                    .AddStep(_deserializer)
                    .AddSteps(_leadingExtractors), ct);

            foreach (var (batchExtractor, then) in _batchExtractors)
            {
                extracted = await BatchStage.ApplyAsync(extracted, batchExtractor, ct, spans);
                if (then.Count > 0)
                    extracted = await PerMessageAsync(extracted, spans, ended, (entry, _, token) =>
                        BulkPipeline.Resume(entry.Result, entry.Context, token).AddSteps(then), ct);
            }

            if (_sender is not null)
                return await PerMessageAsync(extracted, spans, ended, (entry, i, token) =>
                    FinishAsync(Transform(entry, token).AddStep(_sender), spans, ended, i), ct);

            var transformed = await PerMessageAsync(extracted, spans, ended, (entry, _, token) =>
                Transform(entry, token), ct);
            var sent = await BatchStage.ApplyAsync(transformed, _batchSender!, ct, spans);
            return await PerMessageAsync(sent, spans, ended, (entry, i, token) =>
                FinishAsync(BulkPipeline.Resume(entry.Result, entry.Context, token), spans, ended, i), ct);
        }
        catch (Exception e)
        {
            // What ended the batch ended every message still in it: a batch step throwing, or the
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

    /// <summary>Runs a stretch of per-message steps over every message, each under its own span.
    /// Setting the current span in the body is local to that message's run.</summary>
    private Task<TOut[]> PerMessageAsync<TIn, TOut>(IReadOnlyList<TIn> items, Activity?[] spans, bool[] ended,
        Func<TIn, int, CancellationToken, Task<TOut>> chain, CancellationToken ct) =>
        BoundedParallel.RunAsync(items, _degreeOfParallelism, async (item, i, token) =>
        {
            Activity.Current = spans[i];
            try
            {
                return await chain(item, i, token);
            }
            catch (Exception e)
            {
                PipelineTracing.Fail(spans[i], e, _pipelineName, _logger);
                ended[i] = true;
                throw;
            }
        }, ct);

    private Task<(StepResult<TOutput> Result, TCtx Context, CancellationToken CancellationToken)> Transform(
        (StepResult<TInput> Result, TCtx Context, CancellationToken CancellationToken) entry, CancellationToken ct) =>
        BulkPipeline.Resume(entry.Result, entry.Context, ct)
            .AddStep(_validator)
            .AddOptionalStep(_idempotencyChecker)
            .AddStep(_transformer);

    private async Task<(StepResult<TOutput> Result, TCtx Context)> FinishAsync(
        Task<(StepResult<TOutput> Result, TCtx Context, CancellationToken CancellationToken)> sent,
        Activity?[] spans, bool[] ended, int i)
    {
        var pipeline = sent.AddOptionalStep(_idempotencyRecorder);
        if (_receiptSender != null)
            pipeline = pipeline.AddStep(_receiptSender);
        var (result, context, _) = await pipeline.AddFinalizer(_businessIncidentRouter);
        PipelineTracing.Complete(spans[i], result, _logger);
        ended[i] = true;
        return (result, context);
    }
}
