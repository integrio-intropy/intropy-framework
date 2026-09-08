using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;

namespace Intropy.Framework.Testing.Topics;

/// <summary>
/// An enqueue captured by <see cref="FakeEnqueueStep{TCtx}"/>.
/// </summary>
/// <param name="Item">The source item that was enqueued. Item identity survives into the capture,
/// so completer-failure tests can assert what was enqueued even when the pipeline result is a
/// business failure.</param>
/// <param name="Envelope">The encoded structured-mode CloudEvents envelope — the exact bytes the
/// real broker would have received.</param>
[System.Diagnostics.CodeAnalysis.SuppressMessage(
    "Performance", "CA1819:Properties should not return arrays",
    Justification = "Captures the exact wire payload; callers may need the raw bytes. Parity with PublishedMessage.Data.")]
public sealed record CapturedEnqueue(SourceItem Item, byte[] Envelope)
{
    /// <summary>
    /// Decodes <see cref="Envelope"/> as a structured-mode CloudEvents envelope — the encoding the
    /// framework's <see cref="Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps.EnqueueStep{TCtx}"/>
    /// produces. Throws if the payload is not a valid envelope.
    /// </summary>
    /// <returns>The decoded <see cref="CloudEvent"/>.</returns>
    public CloudEvent DecodeCloudEvent() =>
        new JsonEventFormatter().DecodeStructuredModeMessage(
            Envelope, contentType: null, extensionAttributes: null);
}
