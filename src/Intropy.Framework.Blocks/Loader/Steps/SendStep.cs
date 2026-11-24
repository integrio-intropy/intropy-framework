using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Loader.Steps;

/// <summary>
/// A step that sends the serialized bytes to an external system. It resolves the destination from
/// the context and its own configuration, and writes the bytes as they are.
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
/// <typeparam name="TCtx">The type of the context</typeparam>
public abstract class SendStep<TCtx> : BusinessStep<byte[], byte[], TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Send";
}
