using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.BulkLoader.Steps;

/// <summary>
/// A step that sends one message's data to an external system. For one call that covers the whole
/// batch, use <see cref="BatchSendStep{T,TCtx}"/>.
/// </summary>
/// <remarks>
/// A failed send is a business failure, as in a Loader's and a transactional integration's send step: the item
/// did not reach the system it was meant for, which is what a business incident reports. An
/// exception the step does not catch becomes a retriable business failure, so that message is
/// routed as an incident and left for redelivery, while the rest of the batch goes on; override
/// <see cref="BusinessStep{TIn,TOut,TCtx}.ExceptionsAreRetriable"/> to make it terminal, or
/// return <see cref="Intropy.Framework.Core.Pipeline.Abstractions.Results.BusinessStepResult{T}.Failure"/>
/// to decide per message.
/// </remarks>
/// <typeparam name="T">The type of data to send</typeparam>
/// <typeparam name="TCtx">The type of the context</typeparam>
public abstract class SendStep<T, TCtx> : BusinessStep<T, T, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Send";
}
