using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.FilePipeline;
using Intropy.Framework.Hosting.Sweep;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Lifecycle;

/// <summary>
/// The receive side of a Transactional Integration: each source file is run through the receive
/// pipeline, which publishes it to the integration's queue. A file is completed (deleted or
/// archived) only after it is on the queue.
/// </summary>
/// <typeparam name="TCtx">The integration's context type.</typeparam>
internal sealed class TransactionalIntegrationReceiveJob<TCtx>(
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory)
    : FilePipelineJob<IReceivePipeline<TCtx>, SourceItem, TCtx>(provider, frameworkOptions, contextFactory, loggerFactory)
    where TCtx : Context
{
    protected override async Task<StepResult<SourceItem>> RunPipelineAsync(IReceivePipeline<TCtx> pipeline,
        SweptFile file, TCtx context, CancellationToken ct)
    {
        var item = new SourceItem(file.Name, await file.ReadAsync(ct));
        var (result, _) = await pipeline.Execute(item, context, detachTrace: true, ct);
        return result;
    }
}
