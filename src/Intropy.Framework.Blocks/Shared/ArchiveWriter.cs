using System.Diagnostics;
using System.Globalization;
using Intropy.Framework.Blocks.Common;
using Intropy.Framework.Blocks.TransactionalIntegration;

namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Copies an item's payload to the archive store: the part of archiving that is the same whichever
/// step does it, on receipt or on an incident.
/// </summary>
internal static class ArchiveWriter
{
    internal const string RefTag = "intropy.archive.ref";
    internal const string UrlTag = "intropy.archive.url";

    /// <summary>The span covering the store write. Named for the mechanism rather than the step, so
    /// one query finds it in every block.</summary>
    private const string ArchiveSpan = "Archive";

    /// <summary>
    /// Writes <paramref name="payload"/> unchanged to the archive store, puts its key on
    /// <paramref name="context"/> (<see cref="ArchiveContextKeys.Ref"/>), and returns the key.
    /// </summary>
    /// <param name="policy">The archive.</param>
    /// <param name="context">The item's context: its id and date, and where the key is kept.</param>
    /// <param name="payload">The bytes the pipeline received.</param>
    /// <param name="ct">A token that aborts the write.</param>
    /// <returns>The archive key.</returns>
    /// <exception cref="InvalidOperationException">The context names no item.</exception>
    internal static async Task<string> WriteAsync(ArchivePolicy policy, Context context,
        ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        var step = Activity.Current;
        var traceId = step is { TraceId: var id } && id != default ? id.ToHexString() : null;
        var key = ArchiveKey.For(policy.ComponentName, DateOf(context, policy.Time), ItemIdOf(context), traceId);

        // The write gets a span of its own because it is I/O that can be slow or fail, and without one
        // it hides inside the step it runs in.
        using (var activity = ActivitySourceProvider.ActivitySource.StartActivity(ArchiveSpan))
        {
            Tag(activity, policy, key);
            try
            {
                await policy.Store.WriteAsync(key, payload.ToArray(), basePathOverride: null, ct);
            }
            catch (Exception e)
            {
                activity?.SetStatus(ActivityStatusCode.Error, e.Message);
                activity?.AddException(e);
                throw;
            }
        }

        // Also on the step's span, so a trace that samples away the child still names the copy.
        Tag(step, policy, key);
        context.Metadata[ArchiveContextKeys.Ref] = key;
        return key;
    }

    /// <summary>Names the archived copy on <paramref name="activity"/>.</summary>
    internal static void Tag(Activity? activity, ArchivePolicy policy, string key)
    {
        activity?.SetTag(RefTag, key);
        if (policy.UrlOf(key) is { } url)
            activity?.SetTag(UrlTag, url);
    }

    /// <summary>The item's id: the message id of a Transactional Integration's queued message, the
    /// CloudEvent id of a loader's event, the file name of a swept file, or the request id of a
    /// gateway's request. Message ids come first because a queued message also carries the file name
    /// it was received as, and two messages can come from files of the same name.</summary>
    private static string ItemIdOf(Context context) =>
        Get(context, ContextKeys.MessageId)
        ?? Get(context, CloudEventContextKeys.Id)
        ?? Get(context, SourceContextKeys.FileName)
        ?? Get(context, SourceContextKeys.RequestId)
        ?? throw new InvalidOperationException(
            "The item cannot be archived: its context names no message id, event id, file name or request id.");

    /// <summary>When the item was received, else the event's time, else now. The first two are
    /// stable across redeliveries, which is what keeps a redelivered message on its own key.</summary>
    private static DateTimeOffset DateOf(Context context, TimeProvider time) =>
        Parse(Get(context, SourceContextKeys.ReceivedAt))
        ?? Parse(Get(context, CloudEventContextKeys.Time))
        ?? time.GetUtcNow();

    private static DateTimeOffset? Parse(string? value) =>
        DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var date)
            ? date
            : null;

    private static string? Get(Context context, string key) =>
        context.Metadata.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;
}
