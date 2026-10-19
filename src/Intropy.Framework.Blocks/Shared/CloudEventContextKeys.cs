namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Dictionary keys the framework guarantees on the pipeline context for message-driven components
/// (a loader consuming a CloudEvent from its topic).
/// </summary>
/// <remarks>
/// The host projects the message's CloudEvent attributes onto the context before any step runs —
/// before deserialization — so every step, idempotency and incident routing read the message's
/// identity from one place. The payload itself travels as the pipeline's input, not on the context.
/// </remarks>
public static class CloudEventContextKeys
{
    /// <summary>
    /// Context key for the CloudEvent ID.
    /// </summary>
    public const string Id = "cloudevent.id";

    /// <summary>
    /// Context key for the CloudEvent subject. The loader's idempotency uses it as the record ID.
    /// </summary>
    public const string Subject = "cloudevent.subject";

    /// <summary>
    /// Context key for the CloudEvent time, formatted as a UTC ISO 8601 round-trip timestamp
    /// (<c>"O"</c>). The loader's idempotency uses it as the record date.
    /// </summary>
    public const string Time = "cloudevent.time";

    /// <summary>
    /// Context key for the CloudEvent source URI.
    /// </summary>
    public const string Source = "cloudevent.source";

    /// <summary>
    /// Context key for the CloudEvent type.
    /// </summary>
    public const string Type = "cloudevent.type";

    /// <summary>
    /// Context key for the CloudEvent data content type, which tells a deserializer how to read the
    /// payload bytes the pipeline is given.
    /// </summary>
    public const string DataContentType = "cloudevent.datacontenttype";
}
