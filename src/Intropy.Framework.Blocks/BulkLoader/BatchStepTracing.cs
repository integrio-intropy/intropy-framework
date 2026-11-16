using System.Diagnostics;
using Intropy.Framework.Blocks.Common;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;

namespace Intropy.Framework.Blocks.BulkLoader;

/// <summary>
/// The spans of a batch step, such as a batch lookup or a batch send. The step is one call for many
/// messages, each traced in its own trace, so it gets a trace of its own: a root span for the call,
/// linked to every message it carried. Each message in turn gets a span for its part in the call —
/// the call's own start and end, and the verdict for that message — in its own trace, linked back
/// to the call. A message's trace then reads like a single-message load, and each call can be found
/// from any of them.
/// </summary>
internal static class BatchStepTracing
{
    private const string LinkKind = "intropy.link.kind";
    private const string MessageLink = "message";
    private const string BatchLink = "batch";
    private const string BatchMessageCount = "messaging.batch.message_count";
    private const string SucceededCount = "intropy.batch.succeeded_count";
    private const string FailedCount = "intropy.batch.failed_count";

    /// <summary>Starts the span of the call, the root of its own trace, linked to the span of every
    /// message it carries.</summary>
    /// <param name="stepName">The batch step's name.</param>
    /// <param name="messages">The span of each message sent; a null one is not linked.</param>
    /// <param name="start">When the call starts.</param>
    internal static Activity? StartBatch(string stepName, IReadOnlyList<Activity?> messages, DateTimeOffset start)
    {
        // Clear the current span so the call starts a trace of its own rather than joining a
        // message's. The change is local to the caller's async flow.
        Activity.Current = null;

        var tags = new TagList
        {
            { "step.name", stepName },
            { BatchMessageCount, messages.Count }
        };
        var links = messages.OfType<Activity>().Select(m => Link(m.Context, MessageLink)).ToList();
        return ActivitySourceProvider.ActivitySource.StartActivity($"Step.{stepName}", ActivityKind.Internal,
            parentContext: default, tags, links, start);
    }

    /// <summary>Ends the span of the call at <paramref name="end"/>, with how many of its messages
    /// it succeeded and failed for, or the exception that ended it.</summary>
    internal static void EndBatch<T>(Activity? batch, IReadOnlyList<StepResult<T>>? verdicts, Exception? failure,
        DateTimeOffset end)
    {
        if (batch is null)
            return;

        if (verdicts is not null)
        {
            var succeeded = verdicts.Count(v => v is StepResult<T>.Success);
            batch.SetTag(SucceededCount, succeeded);
            batch.SetTag(FailedCount, verdicts.Count - succeeded);
        }

        if (failure is not null)
        {
            batch.AddException(failure);
            batch.SetStatus(ActivityStatusCode.Error, failure.Message);
        }
        else
        {
            batch.SetStatus(ActivityStatusCode.Ok);
        }

        batch.SetEndTime(end.UtcDateTime);
        batch.Dispose();
    }

    /// <summary>Records one message's part in the call in the message's own trace: a span under the
    /// message's span spanning the call, with the call's verdict for the message, linked to the
    /// call's span.</summary>
    /// <param name="stepName">The batch step's name.</param>
    /// <param name="message">The message's span; nothing is recorded when it is null.</param>
    /// <param name="batch">The call's span; not linked when it is null.</param>
    /// <param name="batchSize">How many messages the call carried.</param>
    /// <param name="verdict">The call's verdict for the message; null when the call ended in
    /// <paramref name="failure"/> instead.</param>
    /// <param name="failure">The exception that ended the call, if it threw.</param>
    /// <param name="start">When the call started.</param>
    /// <param name="end">When it ended.</param>
    internal static void RecordMessage<T>(string stepName, Activity? message, Activity? batch, int batchSize,
        StepResult<T>? verdict, Exception? failure, DateTimeOffset start, DateTimeOffset end)
    {
        if (message is null)
            return;

        var tags = new TagList
        {
            { "step.name", stepName },
            { BatchMessageCount, batchSize }
        };
        IEnumerable<ActivityLink>? links = batch is null ? null : [Link(batch.Context, BatchLink)];
        using var span = ActivitySourceProvider.ActivitySource.StartActivity($"Step.{stepName}",
            ActivityKind.Internal, message.Context, tags, links, start);
        if (span is null)
            return;

        if (verdict is not null)
        {
            StepTracing.SetResult(span, verdict);
        }
        else if (failure is not null)
        {
            span.AddException(failure);
            span.SetStatus(ActivityStatusCode.Error, failure.Message);
        }

        span.SetEndTime(end.UtcDateTime);
    }

    private static ActivityLink Link(ActivityContext context, string kind) =>
        new(context, new ActivityTagsCollection { [LinkKind] = kind });
}
