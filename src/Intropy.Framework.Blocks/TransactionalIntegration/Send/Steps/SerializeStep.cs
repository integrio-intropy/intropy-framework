using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;

/// <summary>
/// A step that converts TOutput to a string.
/// </summary>
/// <typeparam name="T">The output type of the pipeline</typeparam>
/// <typeparam name="TCtx"></typeparam>
public abstract class SerializeStep<T, TCtx> : TechnicalStep<T, string, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Serialize";
}