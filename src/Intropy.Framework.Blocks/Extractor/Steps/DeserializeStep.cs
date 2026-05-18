using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// A step that takes a string and converts it into TInput.
/// </summary>
/// <typeparam name="TInput">The input type to the pipeline</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class DeserializeStep<TInput, TCtx> : BusinessStep<string, TInput, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName =>  "Deserialize";
}