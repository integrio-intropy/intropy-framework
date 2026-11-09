using System.Diagnostics.CodeAnalysis;

namespace Intropy.Framework.Hosting.StatefulSweeps;

/// <summary>
/// A source port that is read, not listed: each run reads it once into one message (a database
/// query, an HTTP poll). <see cref="StatefulSweep"/> runs it, and keeps its cursor when it has one.
/// </summary>
/// <remarks>
/// <para>
/// Register an implementation keyed by the port name, the way a file port registers its
/// <c>IFileAdapter</c>, and declare the port as usual (<c>AddSourcePort</c>, a definition's
/// <c>SourcePort</c>, or <c>FileSources.Port</c>). A port with a polled source is swept by
/// <see cref="StatefulSweep"/> instead of the file sweep; the rest of the component (its routes, its
/// pipelines, the job) is unchanged.
/// </para>
/// <para>
/// The source only reads. It never moves the cursor and never writes to the system it reads: the
/// sweep owns the cursor and moves it only after the message has been handled.
/// </para>
/// </remarks>
public interface IPolledSource
{
    /// <summary>The cursor the sweep keeps for this source, or <see langword="null"/> for a source
    /// that reads everything on every run (a full export).</summary>
    CursorOptions? Cursor { get; }

    /// <summary>Reads the source once.</summary>
    /// <param name="range">What to read. Without a cursor both bounds are <see langword="null"/>.
    /// With one, <see cref="CursorRange.From"/> is the committed cursor (exclusive), and
    /// <see cref="CursorRange.To"/>, when set, is a pinned upper bound (inclusive): a previous run
    /// read up to it and its message may already be delivered, so read exactly that range again and
    /// nothing beyond it.</param>
    /// <param name="ct">The host's cancellation token.</param>
    /// <returns>The message to send, or <see langword="null"/> when there is nothing to send this
    /// run (an empty result the source skips, or data that is not available yet).</returns>
    Task<PolledRead?> ReadAsync(CursorRange range, CancellationToken ct);
}

/// <summary>
/// How the sweep keeps a polled source's cursor.
/// </summary>
/// <param name="StoreName">The Dapr state store the cursor is kept in.</param>
/// <param name="Initial">The cursor before the first run, or <see langword="null"/> to read from the
/// beginning.</param>
/// <param name="CanPin">Whether the source can read a bounded range again
/// (<see cref="CursorRange.To"/>). When it can, the sweep saves the upper bound as pending before the
/// message is handled, so a run that fails or crashes after publishing is followed by a run that reads
/// exactly the same range, and the idempotency key absorbs the duplicate. When it cannot (a one-time
/// continuation token), the sweep keeps no pending bound and delivery is plain at-least-once.</param>
public sealed record CursorOptions(string StoreName, string? Initial, bool CanPin = true);

/// <summary>
/// The range a polled source is asked to read.
/// </summary>
/// <param name="From">The committed cursor: read what comes after it. <see langword="null"/> on the
/// first run without an initial cursor, and for a source without a cursor.</param>
/// <param name="To">The pinned upper bound, inclusive, or <see langword="null"/> to read everything
/// after <paramref name="From"/> and report the new cursor.</param>
public sealed record CursorRange(string? From, string? To)
{
    /// <summary>The range of a source without a cursor: everything.</summary>
    public static CursorRange All { get; } = new(null, null);
}

/// <summary>
/// One read of a polled source: the message's content, the cursor after it, and what the source
/// wants on the message's context.
/// </summary>
/// <param name="Content">The message's content.</param>
/// <param name="Cursor">The cursor after this read, opaque to the sweep (a timestamp, a rowversion, an
/// ETag or a page token). <see langword="null"/> when the read did not move it (an empty result that
/// is still sent) or the source has no cursor. Ignored when the range was pinned: the pinned bound is
/// the cursor.</param>
[SuppressMessage("Performance", "CA1819:Properties should not return arrays",
    Justification = "The content is the message payload; byte[] is the shape every pipeline takes.")]
public sealed record PolledRead(byte[] Content, string? Cursor)
{
    /// <summary>Entries for the message's context metadata (such as a row count). The framework's own
    /// keys (the source port, the pickup time, the idempotency key) are set after these and win.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = new Dictionary<string, string>();
}
