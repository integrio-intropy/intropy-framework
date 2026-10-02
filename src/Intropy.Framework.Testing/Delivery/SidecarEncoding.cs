using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Testing.Delivery;

/// <summary>
/// Encodes a <see cref="CloudEvent"/> the way the Dapr sidecar hands a published event to a
/// subscriber over gRPC: the core attributes as fields, the payload as the envelope's <c>data</c>
/// member (a JSON object for an object payload; a string payload arrives quoted, as it does for
/// real), and every other attribute as a string extension.
/// </summary>
internal static class SidecarEncoding
{
    private static readonly HashSet<string> s_fields = new(StringComparer.Ordinal)
    {
        "id", "source", "type", "specversion", "datacontenttype", "data", "data_base64"
    };

    internal static (byte[] Data, IReadOnlyDictionary<string, string> Extensions) Encode(CloudEvent cloudEvent)
    {
        ArgumentNullException.ThrowIfNull(cloudEvent);
        using var envelope = JsonDocument.Parse(CloudEventFormat.Formatter.EncodeStructuredModeMessage(cloudEvent, out _));
        var root = envelope.RootElement;

        var data = root.TryGetProperty("data", out var value)
            ? Encoding.UTF8.GetBytes(value.GetRawText())
            : [];
        var extensions = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var property in root.EnumerateObject())
            if (!s_fields.Contains(property.Name))
                extensions[property.Name] = property.Value.ValueKind == JsonValueKind.String
                    ? property.Value.GetString()!
                    : property.Value.GetRawText();
        return (data, extensions);
    }
}
