using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Loader.Steps;

/// <summary>
/// A step that converts TInput to TOutput.
/// </summary>
/// <typeparam name="TInput">The input type of the pipeline</typeparam>
/// <typeparam name="TOutput">The output type of the pipeline</typeparam>
/// <typeparam name="TCtx">The type of the context</typeparam>
public abstract class TransformStep<TInput, TOutput, TCtx> : TechnicalStep<TInput, TOutput, TCtx>
    where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Transform";
}
