using System.Diagnostics;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// One sweep of one of a run-to-completion component's source ports with a pipeline per item:
/// the port's <see cref="ISourceSweep"/> lists and completes the items, and this runs each item
/// through the component's pipeline. A handled item and an idempotent duplicate are completed; a
/// failed item stays for the next run. It is a part of a run, not the job itself:
/// <see cref="SourcesSweep"/> runs one per port, and the component's <see cref="IJob"/> runs that.
/// </summary>
/// <remarks>
/// <para>
/// The sweep owns what every swept component shares, whatever the kind of source: resolving the
/// pipeline once before anything is read (a misconfigured component fails the run once, not once
/// per item), a fresh context per item with the source's own metadata
/// (<see cref="SweptItem.Metadata"/>), <see cref="SourceContextKeys.SourcePort"/> and
/// <see cref="SourceContextKeys.ReceivedAt"/> set, the pipeline resolved in the item's own scope,
/// and the mapping of pipeline results to sweep outcomes — <see cref="StepResult{T}.Cancelled"/>
/// is a duplicate, <see cref="StepResult{T}.Aborted"/> an interruption that never completes the
/// item. A component supplies how one item is read and executed.
/// </para>
/// <para>
/// Tracing: the source sweep makes each item its own trace, linked to the run's span.
/// Implementations run their pipeline with <c>detachTrace: false</c>, so the pipeline continues
/// the item's trace — next to the item's read and completion — and the trace can continue
/// downstream (through the queue, for a Transactional Integration).
/// </para>
/// <para>
/// Held by a singleton job, it never captures a scoped pipeline. It needs no sidecar; the
/// <see cref="JobRunner"/> owns the sidecar lifecycle, job tracing, and exit codes.
/// </para>
/// </remarks>
/// <typeparam name="TPipeline">The pipeline, resolved per item (<see cref="ResolvePipeline"/>).</typeparam>
/// <typeparam name="TResult">The pipeline's result value.</typeparam>
/// <typeparam name="TCtx">The pipeline context.</typeparam>
internal abstract class PortPipelineSweep<TPipeline, TResult, TCtx>(
    SourcePort source,
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory) : IPortSweep
    where TPipeline : notnull
    where TCtx : Context
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<PortPipelineSweep<TPipeline, TResult, TCtx>>();
    private readonly ISourceSweep _sweep = source.CreateSweep(
        new SourceSweepContext(provider, frameworkOptions.ComponentName, loggerFactory));

    /// <summary>The component's identity, registered with <c>AddIntropyFramework</c>.</summary>
    protected string ComponentName => frameworkOptions.ComponentName;

    /// <inheritdoc />
    public string Port => source.Name;

    /// <inheritdoc />
    /// <exception cref="InvalidOperationException">The source cannot be swept (for example, no
    /// adapter is registered for the port), or the pipeline cannot be composed.</exception>
    public async Task ValidateAsync(CancellationToken ct)
    {
        await _sweep.ValidateAsync(ct);
        await using (var validationScope = provider.CreateAsyncScope())
            _ = ResolvePipeline(validationScope.ServiceProvider);
    }

    /// <inheritdoc />
    /// <returns>Handled items as <c>Processed</c>, duplicates as <c>Skipped</c>, and items left
    /// in the source as <c>Failed</c>. A cancelled sweep returns the counts recorded so far.</returns>
    /// <exception cref="InvalidOperationException">Listing failures propagate; a listing failure
    /// is never reported as an empty successful sweep.</exception>
    public Task<RunSummary> SweepAsync(CancellationToken ct) => _sweep.SweepAsync(HandleItemAsync, ct);

    /// <summary>The pipeline for this port's items, in <paramref name="scope"/>: by default the
    /// component's registered <typeparamref name="TPipeline"/>.</summary>
    protected virtual TPipeline ResolvePipeline(IServiceProvider scope) => scope.GetRequiredService<TPipeline>();

    /// <summary>Reads <paramref name="item"/> and runs it through <paramref name="pipeline"/>.</summary>
    protected abstract Task<StepResult<TResult>> RunPipelineAsync(TPipeline pipeline, SweptItem item, TCtx context,
        CancellationToken ct);

    private async Task<SweepOutcome> HandleItemAsync(SweptItem item, CancellationToken ct)
    {
        var pipeline = ResolvePipeline(item.Services);
        var context = CreateContext(item);
        var result = await RunPipelineAsync(pipeline, item, context, ct);
        switch (result)
        {
            // Success covers both a handled item and a successfully routed business incident.
            case StepResult<TResult>.Success:
                _logger.LogInformation("Handled source item {SourceItem} from {SourcePort}", item.Id, item.Port);
                return SweepOutcome.Consumed;
            case StepResult<TResult>.BusinessFailure businessFailure:
                _logger.LogInformation(
                    "Source item {SourceItem} raised a business incident ({Description}); it was routed, so the item is completed",
                    item.Id, businessFailure.Value.Description);
                return SweepOutcome.Consumed;
            case StepResult<TResult>.Cancelled:
                _logger.LogInformation("Skipping {SourceItem} as a duplicate (idempotency check)", item.Id);
                return SweepOutcome.Duplicate;
            case StepResult<TResult>.Aborted:
                return SweepOutcome.Aborted;
            case StepResult<TResult>.TechnicalFailure failure:
                _logger.LogWarning(failure.Value.Exception,
                    "Processing {SourceItem} failed: {Description}; leaving it for the next run",
                    item.Id, failure.Value.Description);
                // The current span is the item's own (the source sweep's): name the failure on it,
                // which also covers failures returned before the pipeline started.
                Activity.Current?.SetStatus(ActivityStatusCode.Error, failure.Value.Description);
                return SweepOutcome.Failed;
            default:
                _logger.LogWarning("Processing {SourceItem} failed ({ResultType}); leaving it for the next run",
                    item.Id, result.GetType().Name);
                return SweepOutcome.Failed;
        }
    }

    // Source identity is available before any step runs so incident routing can name a poisoned
    // item and its port, and the pickup time before the item is read, so everything downstream —
    // through the queue, on every redelivery — keys on one timestamp instead of the current time.
    // The framework owns these keys, and the source owns its own (a file's name); a
    // caller-provided value is overwritten.
    private TCtx CreateContext(SweptItem item)
    {
        var time = provider.GetService<TimeProvider>() ?? TimeProvider.System;
        var metadata = new Dictionary<string, string>(item.Metadata, StringComparer.Ordinal)
        {
            [SourceContextKeys.SourcePort] = item.Port,
            [SourceContextKeys.ReceivedAt] = time.GetUtcNow().ToString("O"),
        };
        return ContextCreation.Create(contextFactory, metadata, isRetry: false, ComponentName, item.Id);
    }
}
