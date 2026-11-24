using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Loader.Steps;

/// <summary>
/// A step that sends a receipt once the data has been loaded: a notification that a record reached
/// the external system, such as an event on a topic.
/// </summary>
/// <remarks>
/// Unlike <see cref="SendStep{TCtx}"/>, a failed receipt is a technical failure: the record was
/// loaded, so there is nothing wrong with the business data to report, only a notification that did
/// not go out. An exception the step does not catch becomes a
/// <see cref="Intropy.Framework.Core.Pipeline.Abstractions.Failures.TechnicalFailure"/>, and the
/// message is left for redelivery.
/// </remarks>
/// <typeparam name="T">The type of data the receipt is sent for: in a <see cref="Loader{TInput,TOutput,TCtx}"/>, the serialized bytes that were sent.</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class SendReceiptStep<T, TCtx> : TechnicalStep<T, T, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "SendReceipt";
}
