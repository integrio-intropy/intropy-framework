using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;

/// <summary>
/// A step that sends the serialized bytes to a destination.
/// </summary>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class SendStep<TCtx> : BusinessStep<byte[], byte[], TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Send";
}