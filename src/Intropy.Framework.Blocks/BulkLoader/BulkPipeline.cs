using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.BulkLoader;

/// <summary>
/// Pipeline functionality for resuming a per-message chain after a batch step, built entirely on
/// <see cref="Intropy.Framework.Core.Pipeline.Core.Pipeline"/>'s own public surface
/// (<c>Start</c>/<c>AddStep</c>/<c>AddOptionalStep</c>/<c>AddFinalizer</c>) — no changes to
/// <see cref="Intropy.Framework.Core"/> itself.
/// </summary>
public static class BulkPipeline
{
    /// <summary>
    /// Resumes a per-message chain from an already-produced <see cref="StepResult{T}"/> — a batch
    /// step's verdict for this message, or an untouched pass-through of an earlier failure — exactly
    /// as <see cref="Intropy.Framework.Core.Pipeline.Core.Pipeline.Start{T,TCtx}"/> begins one from a
    /// raw value wrapped in <see cref="StepResult{T}.Success"/>. Every
    /// <c>AddStep</c>/<c>AddOptionalStep</c>/<c>AddFinalizer</c> overload applies unchanged to what
    /// this returns.
    /// </summary>
    /// <param name="result">The result to resume the chain from.</param>
    /// <param name="context">The context to resume the chain with.</param>
    /// <param name="ct">A cancellation token that flows through the rest of the chain.</param>
    /// <typeparam name="T">The type of the value carried by <paramref name="result"/>.</typeparam>
    /// <typeparam name="TCtx">The type of the context.</typeparam>
    public static Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> Resume<T, TCtx>(
        StepResult<T> result, TCtx context, CancellationToken ct = default)
    {
        return Task.FromResult((result, context, ct));
    }
}
