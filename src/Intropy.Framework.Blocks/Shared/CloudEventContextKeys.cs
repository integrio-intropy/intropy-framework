namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Dictionary keys used for storing CloudEvent metadata in context.
/// </summary>
internal static class CloudEventContextKeys
{
    /// <summary>
    /// Context key for the CloudEvent ID.
    /// </summary>
    internal const string Id = "cloudevent.id";

    /// <summary>
    /// Context key for the CloudEvent subject.
    /// </summary>
    internal const string Subject = "cloudevent.subject";

    /// <summary>
    /// Context key for the CloudEvent time in ISO 8601 format.
    /// </summary>
    internal const string Time = "cloudevent.time";

    /// <summary>
    /// Context key for the CloudEvent source URI.
    /// </summary>
    internal const string Source = "cloudevent.source";

    /// <summary>
    /// Context key for the CloudEvent type.
    /// </summary>
    internal const string Type = "cloudevent.type";
}
