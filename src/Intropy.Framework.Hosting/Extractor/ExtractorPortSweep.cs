using System.Text;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.FileSweeps;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>
/// The extractor's sweep of one source port: each source file is read as UTF-8 text and run
/// through the composed <see cref="Extractor{TInput,TOutput,TCtx}"/> pipeline.
/// </summary>
internal sealed class ExtractorPortSweep<TInput, TOutput, TCtx>(
    SourcePort source,
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory)
    : FilePipelineSweep<Extractor<TInput, TOutput, TCtx>, CloudEvent, TCtx>(source, provider, frameworkOptions,
        contextFactory, loggerFactory)
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

        var (result, _) = await pipeline.Execute(content, context, detachTrace: false, ct);
        return result;
    }
}
