using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Loader.Steps;

/// <summary>
/// A step that sends data to an external system.
/// </summary>
/// <remarks>
/// A failed send is a business failure, as in a transactional integration's send step: the item
/// did not reach the system it was meant for, which is what a business incident reports. An
/// exception the step does not catch becomes a retriable business failure, so the message is
/// routed as an incident and left for redelivery; override
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
