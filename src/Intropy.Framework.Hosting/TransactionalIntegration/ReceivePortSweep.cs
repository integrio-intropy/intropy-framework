using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Sweeps;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// The receive side's sweep of one source port: each source item is read as raw bytes and run
/// through the receive pipeline, which publishes it to the integration's queue.
/// </summary>
/// <typeparam name="TCtx">The integration's context type.</typeparam>
internal sealed class ReceivePortSweep<TCtx>(
    SourcePort source,
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory)
    : PortPipelineSweep<IReceivePipeline<TCtx>, SourceItem, TCtx>(source, provider, frameworkOptions, contextFactory,
        loggerFactory)
    where TCtx : Context
{
    protected override async Task<StepResult<SourceItem>> RunPipelineAsync(IReceivePipeline<TCtx> pipeline,
        SweptItem item, TCtx context, CancellationToken ct)
    {
        var sourceItem = new SourceItem(item.Id, await item.ReadAsync(ct)) { Port = item.Port };
        var (result, _) = await pipeline.Execute(sourceItem, context, detachTrace: false, ct);
        return result;
    }
}
