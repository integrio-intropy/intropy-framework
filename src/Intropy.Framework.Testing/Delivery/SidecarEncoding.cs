using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;

namespace Intropy.Framework.Testing.Delivery;

/// <summary>
/// Encodes a <see cref="CloudEvent"/> the way the Dapr sidecar hands a published event to a
/// subscriber over gRPC: the core attributes as fields, the payload as the envelope's <c>data</c>
/// member (so a payload published as a JSON string arrives quoted, as it does for real), and every
/// other attribute as a string extension.
/// </summary>
internal static class SidecarEncoding
{
    private static readonly JsonEventFormatter s_formatter = new();

    private static readonly HashSet<string> s_fields = new(StringComparer.Ordinal)
    {
        "id", "source", "type", "specversion", "datacontenttype", "data", "data_base64"
    };

    internal static (byte[] Data, IReadOnlyDictionary<string, string> Extensions) Encode(CloudEvent cloudEvent)
    {
        ArgumentNullException.ThrowIfNull(cloudEvent);
        using var envelope = JsonDocument.Parse(s_formatter.EncodeStructuredModeMessage(cloudEvent, out _));
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
