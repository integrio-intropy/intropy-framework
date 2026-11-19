using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.BulkLoader.Steps;

/// <summary>
/// A step that sends a receipt for one message, once its data has been loaded.
/// </summary>
/// <remarks>
/// Unlike <see cref="SendStep{T,TCtx}"/>, a failed receipt is a technical failure: the record was
/// loaded, so there is nothing wrong with the business data to report, only a notification that did
/// not go out. An exception the step does not catch becomes a
/// <see cref="Intropy.Framework.Core.Pipeline.Abstractions.Failures.TechnicalFailure"/>, and the
/// message is left for redelivery.
/// </remarks>
/// <typeparam name="T">The type of data the receipt is sent for.</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class SendReceiptStep<T, TCtx> : TechnicalStep<T, T, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "SendReceipt";
}
