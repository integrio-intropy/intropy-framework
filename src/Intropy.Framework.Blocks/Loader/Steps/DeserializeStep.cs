using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Loader.Steps;

/// <summary>
/// A step that takes the consumed message's payload and converts it into TInput.
/// </summary>
/// <remarks>
/// The step receives the bytes of the CloudEvent's <c>data</c> member verbatim — exactly what the
/// publisher encoded, with no unwrapping — so a payload published as a JSON object arrives as that
/// object's bytes and one published as a JSON string arrives quoted. The message's own attributes
/// (id, subject, time, source, type, data content type) are on the context before this step runs;
/// see <see cref="CloudEventContextKeys"/>.
/// </remarks>
/// <typeparam name="TInput">The input type to the pipeline</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class DeserializeStep<TInput, TCtx> : BusinessStep<ReadOnlyMemory<byte>, TInput, TCtx>
    where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Deserialize";
}
