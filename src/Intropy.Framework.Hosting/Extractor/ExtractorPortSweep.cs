using System.Text;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.FileSweeps;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>
/// The extractor's sweep of one source port: each source file is read as UTF-8 text and run
/// through the pipeline of the port's route.
/// </summary>
internal sealed class ExtractorPortSweep<TCtx>(
    SourcePort source,
    ExtractorRoute<TCtx> route,
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory)
    : FilePipelineSweep<ExtractorRun<TCtx>, CloudEvent, TCtx>(source, provider, frameworkOptions,
        contextFactory, loggerFactory)
    where TCtx : Context
{
    protected override ExtractorRun<TCtx> ResolvePipeline(IServiceProvider scope) =>
        route.Resolve(scope, ComponentName, Port);

    protected override async Task<StepResult<CloudEvent>> RunPipelineAsync(ExtractorRun<TCtx> pipeline,
        SweptFile file, TCtx context, CancellationToken ct)
    {
        // A listed file with no content is stuck: fail it and keep it so the run exits 1 instead
        // of silently skipping it. An empty file reads as string.Empty on every adapter.
        var content = await file.ReadTextAsync(Encoding.UTF8, ct);
        if (content.Length == 0)
            return new StepResult<CloudEvent>.TechnicalFailure(new TechnicalFailure("The file has no content"));

        return await pipeline(content, context, ct);
    }
}
