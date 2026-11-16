using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Blocks.BulkLoader.Steps;

/// <summary>
/// A pipeline position that sends every message's data to an external system in one call, instead
/// of one call per message — the batch counterpart of
/// <see cref="Intropy.Framework.Blocks.Loader.Steps.SendStep{T,TCtx}"/>. A rejection specific to one
/// entry maps to that entry's own <see cref="Intropy.Framework.Core.Pipeline.Abstractions.Results.StepResult{T}.BusinessFailure"/>.
/// </summary>
/// <typeparam name="T">Per-message input and output type (identity, like
/// <see cref="Intropy.Framework.Blocks.Loader.Steps.SendStep{T,TCtx}"/>).</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class BatchSendStep<T, TCtx> : BatchStep<T, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "BulkSend";
}
