using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.BulkLoader.Steps;

/// <summary>
/// A step that extracts additional data for one message of a batch. For one call that covers the
/// whole batch, use <see cref="BatchExtractStep{TInput,TCtx}"/>.
/// </summary>
/// <typeparam name="TInput">The input type to the pipeline</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class ExtractStep<TInput, TCtx> : BusinessStep<TInput, TInput, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Extract";
}
