using System.Text;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.FilePipeline;
using Intropy.Framework.Hosting.Sweep;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>
/// One run of an extractor: each source file is read as UTF-8 text and run through the composed
/// <see cref="Extractor{TInput,TOutput,TCtx}"/> pipeline. A published file (or one routed as a
/// delivered business incident) and an idempotent duplicate are completed as the source port
/// says (<c>AddSourcePort</c>); a failed file stays for
/// the next scheduled run — the idempotency check makes the retry safe.
/// </summary>
/// <remarks>
/// Delivery semantics: publication happens before the idempotency recording, and delivery is
/// at-least-once, not exactly-once. A crash after publication but before the record means the
/// next run processes the file again and may publish it a second time. (A source file that could
/// not be deleted after a fully recorded success is different: the record exists, so the
/// redelivery is cancelled as a duplicate.)
/// </remarks>
internal sealed class ExtractorJob<TInput, TOutput, TCtx>(
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory)
    : FilePipelineJob<Extractor<TInput, TOutput, TCtx>, CloudEvent, TCtx>(provider, frameworkOptions, contextFactory,
        loggerFactory)
    where TCtx : Context
{
    protected override async Task<StepResult<CloudEvent>> RunPipelineAsync(Extractor<TInput, TOutput, TCtx> pipeline,
        SweptFile file, TCtx context, CancellationToken ct)
    {
        // A listed file with no content is stuck: fail it and keep it so the run exits 1 instead
        // of silently skipping it. An empty file reads as string.Empty on every adapter.
        var content = await file.ReadTextAsync(Encoding.UTF8, ct);
        if (content.Length == 0)
            return new StepResult<CloudEvent>.TechnicalFailure(new TechnicalFailure("The file has no content"));

        var (result, _) = await pipeline.Execute(content, context, detachTrace: true, ct);
        return result;
    }
}
