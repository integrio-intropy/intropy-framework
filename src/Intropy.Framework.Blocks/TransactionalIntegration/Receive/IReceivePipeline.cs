using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive;

/// <summary>
/// The receive side of a Transactional Integration: publishes one source item to the
/// integration's queue. Reading the source and completing it (delete or archive) belong to the
/// file sweep that runs the pipeline, which completes an item only after this pipeline succeeds.
/// </summary>
/// <typeparam name="TCtx">The type of the context used in the pipeline.</typeparam>
public interface IReceivePipeline<TCtx> where TCtx : Context
{
    /// <summary>
    /// Executes the pipeline for a single source item.
    /// </summary>
    /// <param name="item">The source item and its content.</param>
    /// <param name="context">The context to use in the execution.</param>
    /// <param name="detachTrace">When false (the default), the execution continues the ambient
    /// trace — what a host sets up per item (the file's own trace); when true, it starts its own
    /// trace, linked to the current one. Hosts that prepare an item trace pass false explicitly;
    /// standalone callers rarely need to detach.</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution.</param>
    /// <returns>A tuple of the final result of the pipeline execution and the final context.</returns>
    Task<(StepResult<SourceItem> Result, TCtx Context)> Execute(SourceItem item,
        TCtx context, bool detachTrace = false, CancellationToken ct = default);
}
