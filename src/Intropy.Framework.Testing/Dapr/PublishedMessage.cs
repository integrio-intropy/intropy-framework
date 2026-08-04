using System.Text;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;

namespace Intropy.Framework.Testing.Dapr;

/// <summary>
/// A pub/sub publish call captured by <see cref="PublishedMessageCapture"/>.
/// </summary>
/// <param name="PubSubName">The name of the Dapr pub/sub component published to.</param>
/// <param name="TopicName">The topic published to.</param>
/// <param name="ContentType">The content type passed to the publish call.</param>
public sealed record PublishedMessage(
    string PubSubName, string TopicName, string? ContentType)
{
    /// <summary>
    /// The raw payload bytes — a structured-mode CloudEvents envelope when
    /// <see cref="ContentType"/> is <c>application/cloudevents+json</c>.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Performance", "CA1819:Properties should not return arrays",
        Justification = "Captures the exact wire payload; callers may need the raw bytes.")]
    public required byte[] Data { get; init; }
    /// <summary>
    /// Decodes <see cref="Data"/> as a structured-mode CloudEvents envelope — the encoding the
    /// framework's Dapr topic publisher produces. Throws if the payload is not a valid envelope.
    /// </summary>
    /// <returns>The decoded <see cref="CloudEvent"/>.</returns>
    public CloudEvent DecodeCloudEvent() =>
        new JsonEventFormatter().DecodeStructuredModeMessage(
            Data, contentType: null, extensionAttributes: null);

    /// <summary>
    /// Decodes <see cref="Data"/> as a UTF-8 string.
    /// </summary>
    /// <returns>The UTF-8 decoded payload.</returns>
    public string GetDataAsString() => Encoding.UTF8.GetString(Data);
}
