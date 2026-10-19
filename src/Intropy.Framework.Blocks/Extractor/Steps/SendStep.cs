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

    /// <summary>
    /// The envelope attributes this step supplies from its own configuration — the source, and the
    /// type when the serialize step resolved none — or <see langword="null"/> when it supplies
    /// neither.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Both are <em>required</em> attributes that the serialize step cannot provide: the source is
    /// the send step's alone, and the type is its fallback. Declaring them lets anything that has to
    /// encode or measure the envelope before this step runs — the claim check — complete it with the
    /// same values this step would have applied, rather than keeping a second copy of them.
    /// </para>
    /// <para>
    /// The built-in senders declare theirs. A custom sender that declares none can still be used
    /// normally; it just cannot be claim-checked, because the claim check cannot measure an envelope
    /// it cannot encode. Override it to make one claim-checkable.
    /// </para>
    /// </remarks>
    protected internal virtual (Uri Source, string Type)? EnvelopeDefaults => null;
}
