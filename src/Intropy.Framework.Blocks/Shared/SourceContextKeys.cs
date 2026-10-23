namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Dictionary keys the framework guarantees on the pipeline context for source-driven
/// components (e.g. a file sweep feeding an extractor pipeline).
/// </summary>
public static class SourceContextKeys
{
    /// <summary>
    /// The source file name (item identity) of the item being processed. The framework
    /// sets this before any step runs — including deserialization — so business-incident
    /// routing can identify a poisoned source file.
    /// </summary>
    public const string FileName = "file_name";

    /// <summary>
    /// The source port the item being processed was swept from, as declared in the system's
    /// topology. The framework sets this next to <see cref="FileName"/>, before any step runs, so
    /// steps, incident routing and idempotency can tell a component's sources apart.
    /// </summary>
    public const string SourcePort = "source_port";

    /// <summary>
    /// When the framework picked the item up: the moment the sweep created the context, before the
    /// item's content is read and before any step runs. Formatted as a UTC ISO 8601 round-trip
    /// timestamp (<c>"O"</c>), so <see cref="DateTimeOffset.TryParse(string, IFormatProvider, System.Globalization.DateTimeStyles, out DateTimeOffset)"/>
    /// with <see cref="System.Globalization.DateTimeStyles.RoundtripKind"/> reads it back.
    /// </summary>
    /// <remarks>
    /// Set next to <see cref="FileName"/> and <see cref="SourcePort"/>, and propagated with the
    /// message, so idempotency and tracing on the far side of a queue hop can key on when the item
    /// arrived rather than on the current time or a date parsed out of each document.
    /// <para>
    /// The value is stable across redeliveries of the same message: a redelivery restores the
    /// metadata the item was published with. It is not stable across a re-sweep — an item that
    /// failed before it reached the queue stays in the source and is stamped afresh on the next run.
    /// </para>
    /// </remarks>
    public const string ReceivedAt = "received_at";

    /// <summary>
    /// The identity of the HTTP request a gateway received the item in: the caller's
    /// <c>X-Request-Id</c> header, else one the gateway generated. Set next to
    /// <see cref="SourcePort"/> and <see cref="ReceivedAt"/>, before any step runs, and echoed in
    /// the response, so the caller and an incident name the same request.
    /// </summary>
    public const string RequestId = "request_id";
}
