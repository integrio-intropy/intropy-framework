namespace Intropy.Framework.EventDispatcher.Abstractions;

/// <summary>
/// Associates a CloudEvent handler class with a specific event type for automatic discovery and registration.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class CloudEventTypeAttribute : Attribute
{
    /// <summary>
    /// The CloudEvent type identifier this handler is registered for.
    /// </summary>
    public string EventType { get; }

    /// <summary>
    /// Initializes a new instance of <see cref="CloudEventTypeAttribute"/> with the specified event type.
    /// </summary>
    /// <param name="eventType">The CloudEvent type identifier (e.g., "order.created").</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="eventType"/> is null or whitespace.</exception>
    public CloudEventTypeAttribute(string eventType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventType);
        EventType = eventType;
    }
}
