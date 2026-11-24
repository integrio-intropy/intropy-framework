using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Loader.Steps;

/// <summary>
/// A step that converts TOutput to the bytes that are sent. The bytes are what the sender writes
/// as they are, so the step owns the encoding of text formats, and a loader whose transform already
/// produces the bytes passes them through with <see cref="PassThroughSerializer{TCtx}"/>.
/// </summary>
/// <remarks>
/// A failed serialization is a technical failure: writing a document the transform has already
/// built does not depend on the business data. An exception the step does not catch becomes a
/// <see cref="Intropy.Framework.Core.Pipeline.Abstractions.Failures.TechnicalFailure"/>, and the
/// message is left for redelivery.
/// </remarks>
/// <typeparam name="T">The type the transform produces.</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class SerializeStep<T, TCtx> : TechnicalStep<T, byte[], TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Serialize";
}
