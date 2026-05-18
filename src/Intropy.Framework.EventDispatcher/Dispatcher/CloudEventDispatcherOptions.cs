using System.Text.Json;

namespace Intropy.Framework.EventDispatcher.Dispatcher;

/// <summary>
/// Configuration options for <see cref="CloudEventDispatcher"/>.
/// </summary>
public class CloudEventDispatcherOptions
{
    /// <summary>
    /// Optional JSON serialization options used when deserializing CloudEvent data payloads.
    /// When <c>null</c>, the default <see cref="System.Text.Json.JsonSerializer"/> settings are used.
    /// </summary>
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }
}
