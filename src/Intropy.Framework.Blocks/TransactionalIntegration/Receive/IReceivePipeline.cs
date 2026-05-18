using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive;

/// <summary>
/// A pipeline used on the receive side of a Transactional Integration
/// </summary>
/// <typeparam name="TCtx"></typeparam>
public interface IReceivePipeline<TCtx> where TCtx : Context
{
    /// <summary>
    /// Executes the pipeline for a single source item.
    /// </summary>
    /// <param name="itemInfo">Information about the item to process.</param>
    /// <param name="context">The context to use in the execution.</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution.</param>
    /// <returns>A tuple of the final result of the pipeline execution and the final context.</returns>
    Task<(StepResult<SourceItem> Result, TCtx Context)> Execute(SourceItemInfo itemInfo,
        TCtx context, CancellationToken ct = default);
}
