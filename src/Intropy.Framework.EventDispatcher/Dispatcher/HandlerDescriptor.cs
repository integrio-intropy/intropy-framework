namespace Intropy.Framework.EventDispatcher.Dispatcher;

/// <summary>
/// Describes a registered CloudEvent handler, including its event type, handler type, and expected data type.
/// </summary>
public class HandlerDescriptor
{
    /// <summary>
    /// The CloudEvent type identifier this handler is registered for.
    /// </summary>
    public required string EventType { get; init; }

    /// <summary>
    /// The concrete type of the handler class.
    /// </summary>
    public required Type HandlerType { get; init; }

    /// <summary>
    /// The expected data type (<c>TData</c>) for deserialization.
    /// </summary>
    public required Type DataType { get; init; }
}
