using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Blocks.BulkLoader.Steps;

/// <summary>
/// A pipeline position that extracts additional data for every message in one call, instead of one
/// call per message — the batch counterpart of
/// <see cref="Intropy.Framework.Blocks.Loader.Steps.ExtractStep{TInput,TCtx}"/>. The typical use is a
/// batch lookup: messages that carry only a reference to what changed are filled in from a source
/// that can resolve many references at once.
/// </summary>
/// <remarks>
/// Like <see cref="Intropy.Framework.Blocks.Loader.Steps.ExtractStep{TInput,TCtx}"/>, the step returns
/// the same type it is given: model the input so that it has room for what is extracted (a
/// reference, and a data slot the step fills in), and have the deserializer produce it. A reference
/// the source does not resolve maps to that entry's own
/// <see cref="Intropy.Framework.Core.Pipeline.Abstractions.Results.StepResult{T}.BusinessFailure"/>.
/// </remarks>
/// <typeparam name="TInput">The input type of the pipeline.</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class BatchExtractStep<TInput, TCtx> : BatchStep<TInput, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "BulkExtract";
}
