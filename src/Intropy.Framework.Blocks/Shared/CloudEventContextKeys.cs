namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Dictionary keys the framework guarantees on the pipeline context for message-driven components
/// (a loader consuming a CloudEvent from its topic).
/// </summary>
/// <remarks>
/// The host projects the message's CloudEvent attributes onto the context before any step runs —
/// before deserialization — so every step, idempotency and incident routing read the message's
/// identity from one place. The payload itself travels as the pipeline's input, not on the context.
/// Every extension attribute the message carries is projected too, under <see cref="Extension"/>,
/// so a step reads a publisher's own attributes (a tenant, a correlation id) without going back to
/// the payload for them.
/// </remarks>
public static class CloudEventContextKeys
{
    /// <summary>
    /// The prefix every key in this class shares, and that <see cref="Extension"/> puts in front of
    /// an extension attribute's name.
    /// </summary>
    public const string Prefix = "cloudevent.";

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

    /// <summary>
    /// Context key for the CloudEvent extension attribute <paramref name="name"/>, such as
    /// <c>cloudevent.tenantid</c> for a <c>tenantid</c> extension. The value is the attribute's
    /// string form; an attribute the message does not carry has no key.
    /// </summary>
    /// <remarks>
    /// Subject and time arrive as extensions on some transports, so <c>Extension("subject")</c> and
    /// <c>Extension("time")</c> are the same keys as <see cref="Subject"/> and <see cref="Time"/>,
    /// holding the same values — time normalised as <see cref="Time"/> describes.
    /// </remarks>
    /// <param name="name">The extension attribute's name, as it appears on the wire (CloudEvents
    /// attribute names are lower-case).</param>
    public static string Extension(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return Prefix + name;
    }
}
