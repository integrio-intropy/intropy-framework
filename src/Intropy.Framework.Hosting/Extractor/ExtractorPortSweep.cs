using System.Text;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Sweeps;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>
/// The extractor's sweep of one source port: each source item is read as raw bytes and run
/// through the pipeline of the port's route.
/// </summary>
internal sealed class ExtractorPortSweep<TCtx>(
    SourcePort source,
    ExtractorRoute<TCtx> route,
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory)
    : PortPipelineSweep<ExtractorRun<TCtx>, CloudEvent, TCtx>(source, provider, frameworkOptions,
        contextFactory, loggerFactory)
    where TCtx : Context
{
    protected override ExtractorRun<TCtx> ResolvePipeline(IServiceProvider scope) =>
        route.Resolve(scope, ComponentName, Port);

    protected override async Task<StepResult<CloudEvent>> RunPipelineAsync(ExtractorRun<TCtx> pipeline,
        SweptItem item, TCtx context, CancellationToken ct)
    {
        // An item with no content has nothing to extract: fail it and keep it so the run exits 1
        // instead of silently skipping it. An empty file reads as an empty array on every adapter.
        var content = await item.ReadAsync(ct);
        if (content.Length == 0)
            return new StepResult<CloudEvent>.TechnicalFailure(new TechnicalFailure("The source item has no content"));

        return await pipeline(content, context, ct);
    }
}
