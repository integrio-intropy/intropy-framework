using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Loader.Steps;

/// <summary>
/// A serialize step for a loader whose transform already produces the bytes to send: it returns
/// its input untouched.
/// </summary>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public sealed class PassThroughSerializer<TCtx> : SerializeStep<byte[], TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override Task<(TechnicalStepResult<byte[]> Result, TCtx Context)> ExecuteAsync(byte[] input,
        TCtx context, CancellationToken ct) =>
        Task.FromResult<(TechnicalStepResult<byte[]>, TCtx)>((new TechnicalStepResult<byte[]>.Success(input), context));
}
