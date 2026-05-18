using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;

/// <summary>
/// A step that can be used to extract additional data.
/// </summary>
/// <typeparam name="TInput">The input type to the pipeline</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class ExtractStep<TInput, TCtx> : BusinessStep<TInput, TInput, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Extract";
}