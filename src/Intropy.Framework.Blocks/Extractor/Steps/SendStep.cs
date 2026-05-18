using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// A step that sends the serialized data to a destination.
/// </summary>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class SendStep<TCtx> : TechnicalStep<CloudEvent, CloudEvent, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Send";
}