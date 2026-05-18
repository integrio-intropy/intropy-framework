using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Send;

/// <summary>
/// A pipeline used on the send side of a Transactional Integration
/// </summary>
/// <typeparam name="TCtx"></typeparam>
public interface ISendPipeline<TCtx> where TCtx : Context
{
    /// <summary>
    /// Executes the pipeline
    /// </summary>
    /// <param name="input">The data to process, represented as string.</param>
    /// <param name="context">The context to use in the execution.</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution.</param>
    /// <returns>A tuple of the final result of the pipeline execution and the final context.</returns>
    Task<(StepResult<string> Result, TCtx Context)> Execute(ReadOnlyMemory<byte> input, TCtx context,
        CancellationToken ct = default);
}
