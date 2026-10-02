using System.Text.Json;
using CloudNative.CloudEvents.SystemTextJson;

namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// The framework's CloudEvent wire format: structured-mode JSON with the payload embedded as a
/// JSON value in <c>data</c>, its property names camelCase (<c>{"orderId": …}</c>), so sidecar
/// rules and non-.NET consumers read the payload the way JSON is usually written. Every block that
/// puts a CloudEvent on the wire encodes it with this formatter.
/// </summary>
public static class CloudEventFormat
{
    /// <summary>The formatter that encodes the framework's CloudEvents.</summary>
    public static JsonEventFormatter Formatter { get; } =
        new(new JsonSerializerOptions(JsonSerializerDefaults.Web), new JsonDocumentOptions());
}
