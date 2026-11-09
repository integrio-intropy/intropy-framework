using System.Diagnostics;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.StatefulSweeps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// One sweep of one of a file-driven component's source ports with a pipeline per file:
/// <see cref="FileSweep"/> lists the port and completes each file (or, for a polled source,
/// <see cref="StatefulSweep"/> reads it and moves its cursor), and this runs each file
/// through the component's pipeline. A handled file and an idempotent duplicate are completed; a
/// failed file stays for the next run. It is a part of a run, not the job itself:
/// <see cref="SourceSweep"/> runs one per port, and the component's <see cref="IJob"/> runs that.
/// </summary>
/// <remarks>
/// <para>
/// The sweep owns what every file-driven component shares: resolving the pipeline once before any
/// file is listed (a misconfigured component fails the run once, not once per file), a fresh
/// context per file with <see cref="SourceContextKeys.FileName"/>,
/// <see cref="SourceContextKeys.SourcePort"/> and <see cref="SourceContextKeys.ReceivedAt"/> set,
/// the pipeline resolved in the file's own scope, and the mapping of pipeline results to sweep
/// outcomes —
/// <see cref="StepResult{T}.Cancelled"/> is a duplicate, <see cref="StepResult{T}.Aborted"/> an
/// interruption that never completes the file. A component supplies how one file is read and
/// executed.
/// </para>
/// <para>
/// Tracing: <see cref="FileSweep"/> makes each file its own trace, linked to the run's span.
/// Implementations run their pipeline with <c>detachTrace: false</c>, so the pipeline continues
/// the file's trace — next to the file's read and completion — and the trace can continue
/// downstream (through the queue, for a Transactional Integration).
/// </para>
/// <para>
/// Held by a singleton job, it never captures a scoped pipeline. It needs no sidecar; the
/// <see cref="JobRunner"/> owns the sidecar lifecycle, job tracing, and exit codes.
/// </para>
/// </remarks>
/// <typeparam name="TPipeline">The pipeline, resolved per file (<see cref="ResolvePipeline"/>).</typeparam>
/// <typeparam name="TResult">The pipeline's result value.</typeparam>
/// <typeparam name="TCtx">The pipeline context.</typeparam>
internal abstract class FilePipelineSweep<TPipeline, TResult, TCtx>(
    SourcePort source,
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory) : IPortSweep
    where TPipeline : notnull
    where TCtx : Context
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<FilePipelineSweep<TPipeline, TResult, TCtx>>();

    /// <summary>The component's identity, registered with <c>AddIntropyFramework</c>.</summary>
    protected string ComponentName => frameworkOptions.ComponentName;

    /// <inheritdoc />
    public string Port => source.Name;

    /// <inheritdoc />
    /// <exception cref="InvalidOperationException">Neither a source adapter nor a polled source is
    /// registered for the port, or both are, or the pipeline cannot be composed.</exception>
    public async Task ValidateAsync()
    {
        var file = provider.GetKeyedService<IFileAdapter>(source.Name);
        var polled = provider.GetKeyedService<IPolledSource>(source.Name);
        if (file is null && polled is null)
            throw new InvalidOperationException(
                $"Component '{ComponentName}': no IFileAdapter or IPolledSource is registered for the source port '{source.Name}'.");
        if (file is not null && polled is not null)
            throw new InvalidOperationException(
                $"Component '{ComponentName}': both an IFileAdapter and an IPolledSource are registered for the source " +
                $"port '{source.Name}'; a port has one source.");
        await using (var validationScope = provider.CreateAsyncScope())
            _ = ResolvePipeline(validationScope.ServiceProvider);
    }

    /// <inheritdoc />
    /// <remarks>A port with a polled source (<see cref="IPolledSource"/>, such as a database query) is
    /// read once by <see cref="StatefulSweep"/>; any other port is listed by <see cref="FileSweep"/>.
    /// Either way, each item goes through the same pipeline.</remarks>
    /// <returns>Handled files as <c>Processed</c>, duplicates as <c>Skipped</c>, and files left
    /// in place as <c>Failed</c>. A cancelled sweep returns the counts recorded so far.</returns>
    /// <exception cref="InvalidOperationException">Listing failures propagate; a listing failure
    /// is never reported as an empty successful sweep.</exception>
    public Task<RunSummary> SweepAsync(CancellationToken ct) =>
        provider.GetKeyedService<IPolledSource>(source.Name) is not null
            ? new StatefulSweep(provider, source.Name, ComponentName, loggerFactory).SweepAsync(HandleFileAsync, ct)
            : new FileSweep(provider, source.Name, ComponentName, source.Completion, loggerFactory)
                .SweepAsync(HandleFileAsync, ct);

    /// <summary>The pipeline for this port's files, in <paramref name="scope"/>: by default the
    /// component's registered <typeparamref name="TPipeline"/>.</summary>
    protected virtual TPipeline ResolvePipeline(IServiceProvider scope) => scope.GetRequiredService<TPipeline>();

    /// <summary>Reads <paramref name="file"/> and runs it through <paramref name="pipeline"/>.</summary>
    protected abstract Task<StepResult<TResult>> RunPipelineAsync(TPipeline pipeline, SweptFile file, TCtx context,
        CancellationToken ct);

    private async Task<FileOutcome> HandleFileAsync(SweptFile file, CancellationToken ct)
    {
        var pipeline = ResolvePipeline(file.Services);
        var context = CreateContext(file);
        var result = await RunPipelineAsync(pipeline, file, context, ct);
        switch (result)
        {
            // Success covers both a handled item and a successfully routed business incident.
            case StepResult<TResult>.Success:
                _logger.LogInformation("Handled source file {FileName}", file.Name);
                return FileOutcome.Consumed;
            case StepResult<TResult>.BusinessFailure businessFailure:
                _logger.LogInformation(
                    "Source file {FileName} raised a business incident ({Description}); it was routed, so the file is completed",
                    file.Name, businessFailure.Value.Description);
                return FileOutcome.Consumed;
            case StepResult<TResult>.Cancelled:
                _logger.LogInformation("Skipping {FileName} as a duplicate (idempotency check)", file.Name);
                return FileOutcome.Duplicate;
            case StepResult<TResult>.Aborted:
                return FileOutcome.Aborted;
            case StepResult<TResult>.TechnicalFailure failure:
                _logger.LogWarning(failure.Value.Exception,
                    "Processing {FileName} failed: {Description}; leaving it for the next run",
                    file.Name, failure.Value.Description);
                // The current span is the file's own (FileSweep): name the failure on it, which
                // also covers failures returned before the pipeline started.
                Activity.Current?.SetStatus(ActivityStatusCode.Error, failure.Value.Description);
                return FileOutcome.Failed;
            default:
                _logger.LogWarning("Processing {FileName} failed ({ResultType}); leaving it for the next run",
                    file.Name, result.GetType().Name);
                return FileOutcome.Failed;
        }
    }

    // Source identity is available before any step runs so incident routing can name a poisoned
    // file and its port, and the pickup time before the file is read, so everything downstream —
    // through the queue, on every redelivery — keys on one timestamp instead of the current time.
    // The framework owns these keys; a caller-provided value is overwritten.
    // A polled source's read has no file name; it brings the metadata its source set instead.
    private TCtx CreateContext(SweptFile file)
    {
        var time = provider.GetService<TimeProvider>() ?? TimeProvider.System;
        var metadata = new Dictionary<string, string>(file.Metadata)
        {
            [SourceContextKeys.SourcePort] = file.Port,
            [SourceContextKeys.ReceivedAt] = time.GetUtcNow().ToString("O"),
        };
        if (file.IsFile)
            metadata[SourceContextKeys.FileName] = file.Name;
        return ContextCreation.Create(contextFactory, metadata, isRetry: false, ComponentName, file.Name);
    }
}
