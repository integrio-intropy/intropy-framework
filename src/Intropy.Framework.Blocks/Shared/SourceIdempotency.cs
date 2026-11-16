using System.Globalization;

namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Idempotency keyed on the context metadata a polled source's sweep sets
/// (<see cref="SourceContextKeys.IdempotencyKey"/>) rather than on the payload: the id is the range the
/// item covers, the date is when it was picked up, and the hash is constant, because the same range
/// is the same message even when a re-read returns slightly different rows.
/// </summary>
internal static class SourceIdempotency
{
    /// <summary>The hash every item checked this way carries: the id alone decides.</summary>
    internal const string RangeHash = "source-range";

    internal static string Id(Context context) =>
        context.Metadata.TryGetValue(SourceContextKeys.IdempotencyKey, out var key) && key.Length > 0
            ? key
            : throw new InvalidOperationException(
                $"The context has no '{SourceContextKeys.IdempotencyKey}': only a source with a cursor (such as a " +
                "database query with a delta) sets one. A full export needs no idempotency; remove WithSourceIdempotency.");

    internal static DateTimeOffset Date(Context context) =>
        context.Metadata.TryGetValue(SourceContextKeys.ReceivedAt, out var receivedAt)
        && DateTimeOffset.TryParse(receivedAt, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var date)
            ? date
            : throw new InvalidOperationException(
                $"The context has no valid '{SourceContextKeys.ReceivedAt}'; the source sweep sets it.");
}
