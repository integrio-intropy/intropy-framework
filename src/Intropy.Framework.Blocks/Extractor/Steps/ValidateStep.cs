using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// A step that performs validation of the input data.
/// </summary>
/// <typeparam name="T">The type of the input data</typeparam>
/// <typeparam name="TCtx">The type of the context</typeparam>
public abstract class ValidateStep<T, TCtx> : BusinessStep<T, T, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Validate";
}