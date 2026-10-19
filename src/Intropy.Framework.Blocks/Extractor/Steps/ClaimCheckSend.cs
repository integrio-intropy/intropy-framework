using System.Diagnostics;
using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// Moves a payload too large for the broker into the claim store, leaving the event carrying a
/// pointer to it instead (the claim check pattern), then sends it with the configured send step.
/// </summary>
/// <remarks>
/// <para>
/// It is a decorator rather than a pipeline step because <see cref="ExtractorBuilder{TInput,TOutput,TCtx}"/>
/// composes a fixed pipeline with no arbitrary insertion point — the same shape
/// <see cref="SourcePortStamp{TCtx}"/> uses. It is total: below the threshold the event passes
/// through untouched, so it needs no conditional composition.
/// </para>
/// <para>
/// The payload is written before the event carrying its pointer is published, so a crash can leave
/// an orphaned payload but never a pointer to a payload that was never written. The key is unique
/// per event — <c>{component}/{yyyy}/{MM}/{dd}/{id}</c> — so a dead-lettered message's payload is
/// findable; the date segments partition the store for the lifecycle policy that expires payloads,
/// which is the platform's to own. The framework never deletes.
/// </para>
/// <para>
/// Unlike the Transactional Integration's claim check, this one publishes onto the <em>system
/// bus</em>: the consumers are other components, each of which needs the store configured and
/// reachable before this one starts claim-checking. A claim-checked event also carries no
/// <c>data</c>, so a subscription rule filtering on <c>data.*</c> stops matching it — route on
/// extension attributes instead.
/// </para>
/// </remarks>
/// <typeparam name="TCtx">The type of the context used in the step.</typeparam>
/// <param name="sender">The send step the event is handed to once its payload has been settled.</param>
/// <param name="store">The claim store the payload is written to. Written through its configured base
/// path, never a per-write override: <see cref="IFileAdapter.GetContentAsync"/> has no equivalent
/// override, so a payload written elsewhere could not be read back.</param>
/// <param name="policy">When a payload is claim-checked.</param>
/// <param name="options">The framework options, for the component name in the key.</param>
/// <param name="envelope">The envelope attributes <paramref name="sender"/> would supply, needed to
/// complete the envelope before it can be measured. Taken from the sender itself
/// (<see cref="SendStep{TCtx}.EnvelopeDefaults"/>) so the measured envelope and the published one
/// cannot disagree.</param>
/// <param name="timeProvider">Supplies the date the key is partitioned by. Defaults to
/// <see cref="TimeProvider.System"/>.</param>
internal sealed class ClaimCheckSend<TCtx>(
    SendStep<TCtx> sender,
    IFileAdapter store,
    ClaimCheckPolicy policy,
    FrameworkOptions options,
    (Uri Source, string Type) envelope,
    TimeProvider? timeProvider = null) : SendStep<TCtx>
    where TCtx : Context
{
    private static readonly CloudEventAttribute s_dataRef =
        CloudEventAttribute.CreateExtension(CloudEventExtensionKeys.DataRef, CloudEventAttributeType.String);

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <inheritdoc />
    public override string StepName => sender.StepName;

    /// <inheritdoc />
    protected internal override (Uri Source, string Type)? EnvelopeDefaults => envelope;

    /// <inheritdoc />
    public override async Task<(TechnicalStepResult<CloudEvent> Result, TCtx Context)> ExecuteAsync(CloudEvent input,
        TCtx context, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(input);

        if (input.Data is null)
            return await sender.ExecuteAsync(input, context, ct);

        // The envelope has to be complete before it can be encoded at all, and measuring encodes it.
        // The send step applies the same rule again, to the same values.
        EnvelopeCompletion.Apply(input, envelope.Source, envelope.Type);

        // The id names the key, so an event without one gets one here rather than at publish time:
        // the pointer and the message it travels on must agree. CloudEventSerializeStep already
        // mints one, but a component-supplied serialize step need not.
        if (string.IsNullOrEmpty(input.Id))
            input.Id = Guid.NewGuid().ToString();

        // An id the key format cannot hold is refused by name, here, rather than as an
        // ArgumentException about a "segment" from inside ClaimKey. Deliberately before the size
        // decision below, so every event carrying data is checked rather than only the ones large
        // enough to be claim-checked: an id scheme this pipeline cannot use is a configuration
        // mistake, and finding it on the first event beats finding it weeks later, the first time a
        // payload happens to grow past the threshold.
        if (!ClaimKey.CanName(input.Id))
            throw new InvalidOperationException(
                $"CloudEvent id '{input.Id}' cannot be claim-checked: the id names the payload's key in the " +
                "claim store, so it must be one or more letters, digits, '.', '-' or '_', and neither '.' nor " +
                "'..'. Give the serialize step an id of that shape — the built-in CloudEventSerializeStep's " +
                "GUIDs already are — or turn the claim check off for this extractor.");

        var (payload, wireLength, binary) = Encode(input.Data, input.DataContentType);

        var activity = Activity.Current;
        activity?.SetTag("intropy.claim_check.payload_size", payload.Length);

        if (!WouldExceed(input, wireLength, binary))
        {
            // Small enough to ride inside the envelope: the event passes through unchanged.
            activity?.SetTag("intropy.claim_check", false);
            return await sender.ExecuteAsync(input, context, ct);
        }

        var key = ClaimKey.For(options.ComponentName, _time.GetUtcNow(), input.Id);
        activity?.SetTag("intropy.claim_check", true);
        activity?.SetTag("intropy.claim_check.key", key);

        await store.WriteAsync(key, payload, basePathOverride: null, ct);

        // Pointed at and cleared: the encoder omits an absent payload either way, but clearing it
        // means a wrong branch here publishes a small message rather than the oversized one this
        // whole mechanism exists to prevent. DataContentType stays — it still describes what the
        // store holds, and the consumer projects it onto the pipeline context.
        input[s_dataRef] = key;
        input.Data = null;

        return await sender.ExecuteAsync(input, context, ct);
    }

    /// <summary>What the store must hold, how much the envelope would cost, and whether that cost
    /// is base64.</summary>
    /// <remarks>
    /// <para>
    /// The stored bytes are what a consumer would have received inline — not necessarily what the
    /// envelope carries, because the two differ for a string payload. That equivalence is the whole
    /// contract, so every shape the formatter or the sidecar treats specially has to be matched
    /// here rather than assumed.
    /// </para>
    /// <para>
    /// A <see cref="byte"/> array is the only shape the formatter moves to <c>data_base64</c>, which
    /// costs a third more on the wire and so is measured as base64. A <see cref="ReadOnlyMemory{T}"/>
    /// of bytes is <em>not</em> — the formatter has no case for it, so it serializes like any other
    /// value into <c>data</c> as a base64 JSON string, quotes included.
    /// </para>
    /// <para>
    /// A <see cref="string"/> is the case where stored and wire bytes diverge. The formatter always
    /// writes it as a JSON string, but the sidecar hands a subscriber the <em>raw text</em> back
    /// whenever the content type is not one it reads as JSON — verified against a real sidecar in
    /// <c>LoaderCallbackDaprTests.AStringPayload_IsDeliveredRaw_UnlessItsContentTypeIsJson</c>. So
    /// such a payload is stored raw, while the threshold is still measured against the JSON string
    /// the envelope actually carries.
    /// </para>
    /// </remarks>
    private static (byte[] Store, int WireLength, bool Binary) Encode(object data, string? dataContentType)
    {
        if (data is byte[] bytes)
            return (bytes, bytes.Length, true);

        var json = JsonSerializer.SerializeToUtf8Bytes(data, CloudEventFormat.SerializerOptions);
        return data is string text && !IsJson(dataContentType)
            ? (Encoding.UTF8.GetBytes(text), json.Length, false)
            : (json, json.Length, false);
    }

    /// <summary>Whether the sidecar would read a payload of this content type as JSON, and so leave
    /// a string payload as the JSON string the formatter wrote.</summary>
    /// <remarks>No content type is treated as JSON: the formatter writes JSON regardless, and the
    /// framework's own serialize step defaults the attribute to <c>application/json</c>.</remarks>
    private static bool IsJson(string? dataContentType)
    {
        if (string.IsNullOrEmpty(dataContentType))
            return true;

        var media = dataContentType.AsSpan();
        var parameters = media.IndexOf(';');
        if (parameters >= 0)
            media = media[..parameters];
        media = media.Trim();

        return media.Equals("application/json", StringComparison.OrdinalIgnoreCase)
               || media.EndsWith("+json", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Whether the payload must be claim-checked, measured against the envelope as it
    /// would be published but without its data.</summary>
    /// <remarks><see cref="CloudEvent"/> cannot be copied, so the data is lifted off for the
    /// measurement and put back: the policy must not see it, and nothing else may notice it was
    /// gone.</remarks>
    private bool WouldExceed(CloudEvent input, int payloadLength, bool binary)
    {
        var data = input.Data;
        input.Data = null;
        try
        {
            return policy.WouldExceed(input, payloadLength, binary);
        }
        finally
        {
            input.Data = data;
        }
    }
}
