using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// A step that takes the raw bytes of the source item and converts them into TInput. The bytes are
/// not decoded before this step, so the step owns the encoding: decode text with the encoding the
/// source uses, or hand the bytes to a parser that reads it itself (<c>JsonSerializer</c> reads UTF-8,
/// <c>XmlReader</c> and <c>StreamReader</c> detect a BOM or declaration).
/// </summary>
/// <typeparam name="TInput">The input type to the pipeline</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class DeserializeStep<TInput, TCtx> : BusinessStep<ReadOnlyMemory<byte>, TInput, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName =>  "Deserialize";
}