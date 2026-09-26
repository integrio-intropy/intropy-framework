using System.Diagnostics;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.RunToCompletion;
using Intropy.Framework.Hosting.Sweep;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.FilePipeline;

/// <summary>
/// One run of a file-driven component: <see cref="FileSweep"/> lists the source and completes
/// each file, and this job runs each file through the component's pipeline. A handled file and an
/// idempotent duplicate are completed; a failed file stays for the next run.
/// </summary>
/// <remarks>
/// <para>
/// The job owns what every file-driven component shares: resolving the pipeline once before any
/// file is listed (a misconfigured component fails the run once, not once per file), a fresh
/// context per file with <see cref="SourceContextKeys.FileName"/> set, the pipeline resolved in
/// the file's own scope, and the mapping of pipeline results to sweep outcomes —
/// <see cref="StepResult{T}.Cancelled"/> is a duplicate, <see cref="StepResult{T}.Aborted"/> an
/// interruption that never completes the file. A component supplies how one file is read and
/// executed.
/// </para>
/// <para>
/// Tracing: <see cref="FileSweep"/> makes each file its own trace, linked to the job's span.
/// Implementations run their pipeline with <c>detachTrace: false</c>, so the pipeline continues
/// the file's trace — next to the file's read and completion — and the trace can continue
/// downstream (through the queue, for a Transactional Integration).
/// </para>
/// <para>
/// Registered as a singleton, it never captures a scoped pipeline. It needs no sidecar; the
/// <see cref="RunToCompletionRunner"/> owns the sidecar lifecycle, job tracing, and exit codes.
/// </para>
/// </remarks>
/// <typeparam name="TPipeline">The pipeline service, resolved per file.</typeparam>
/// <typeparam name="TResult">The pipeline's result value.</typeparam>
/// <typeparam name="TCtx">The pipeline context.</typeparam>
internal abstract class FilePipelineJob<TPipeline, TResult, TCtx>(
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory) : IRunToCompletionJob
    where TPipeline : notnull
    where TCtx : Context
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<FilePipelineJob<TPipeline, TResult, TCtx>>();

    /// <summary>The component's identity, registered with <c>AddIntropyFramework</c>.</summary>
    private string ComponentName => frameworkOptions.ComponentName;

    /// <summary>Sweeps the source once.</summary>
    /// <returns>Handled files as <c>Processed</c>, duplicates as <c>Skipped</c>, and files left
    /// in place as <c>Failed</c>. A cancelled sweep returns the counts recorded so far.</returns>
    /// <exception cref="InvalidOperationException">Thrown before any file is listed when no
    /// component identity, source port (<c>AddSourcePort</c>) or source adapter is registered, or the
    /// pipeline cannot be composed. Listing failures propagate too; a listing failure is never
    /// reported as an empty successful sweep.</exception>
    public async Task<JobRunSummary> ExecuteAsync(CancellationToken ct)
    {
        var componentName = ComponentName;
        var source = provider.GetService<SourcePort>() ?? throw new InvalidOperationException(
            $"Component '{componentName}': no source port is registered. Call AddSourcePort.");
        _ = provider.GetKeyedService<IFileAdapter>(source.Name) ?? throw new InvalidOperationException(
            $"Component '{componentName}': no IFileAdapter is registered for the source port '{source.Name}'.");
        await using (var validationScope = provider.CreateAsyncScope())
            _ = validationScope.ServiceProvider.GetRequiredService<TPipeline>();

        var sweep = new FileSweep(provider, source.Name, componentName, source.Completion, loggerFactory);
        return await sweep.SweepAsync(HandleFileAsync, ct);
    }

    /// <summary>Reads <paramref name="file"/> and runs it through <paramref name="pipeline"/>.</summary>
    protected abstract Task<StepResult<TResult>> RunPipelineAsync(TPipeline pipeline, SweptFile file, TCtx context,
        CancellationToken ct);

    private async Task<SweepOutcome> HandleFileAsync(SweptFile file, CancellationToken ct)
    {
        var pipeline = file.Services.GetRequiredService<TPipeline>();
        var context = CreateContext(file.Name);
        var result = await RunPipelineAsync(pipeline, file, context, ct);
        switch (result)
        {
            // Success covers both a handled item and a successfully routed business incident.
            case StepResult<TResult>.Success:
                _logger.LogInformation("Handled source file {FileName}", file.Name);
                return SweepOutcome.Consumed;
            case StepResult<TResult>.Cancelled:
                _logger.LogInformation("Skipping {FileName} as a duplicate (idempotency check)", file.Name);
                return SweepOutcome.Duplicate;
            case StepResult<TResult>.Aborted:
                return SweepOutcome.Aborted;
            case StepResult<TResult>.TechnicalFailure failure:
                _logger.LogWarning(failure.Value.Exception,
                    "Processing {FileName} failed: {Description}; leaving it for the next run",
                    file.Name, failure.Value.Description);
                // The current span is the file's own (FileSweep): name the failure on it, which
                // also covers failures returned before the pipeline started.
                Activity.Current?.SetStatus(ActivityStatusCode.Error, failure.Value.Description);
                return SweepOutcome.Failed;
            default:
                _logger.LogWarning("Processing {FileName} failed ({ResultType}); leaving it for the next run",
                    file.Name, result.GetType().Name);
                return SweepOutcome.Failed;
        }
    }

    // Source identity is available before any step runs so incident routing can name a poisoned
    // file. The framework owns this key; a caller-provided value is overwritten.
    private TCtx CreateContext(string fileName) =>
        ContextCreation.Create(contextFactory,
            new Dictionary<string, string> { [SourceContextKeys.FileName] = fileName },
            isRetry: false, ComponentName, fileName);
}
