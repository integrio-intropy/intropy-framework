using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;

/// <summary>
/// A step that converts TOutput to the bytes that are sent. The bytes are what the sender writes
/// as they are, so the step owns the encoding of text formats and a file-to-file flow can pass
/// bytes through untouched.
/// </summary>
/// <typeparam name="T">The output type of the pipeline</typeparam>
/// <typeparam name="TCtx"></typeparam>
public abstract class SerializeStep<T, TCtx> : TechnicalStep<T, byte[], TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Serialize";
}