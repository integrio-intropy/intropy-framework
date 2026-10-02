using System.Diagnostics;
using System.Globalization;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Common;

namespace Intropy.Framework.Hosting.Messaging;

internal static class MessageActivity
{
    private const string TraceParentKey = "traceparent";
    private const string TraceStateKey = "tracestate";
    private const string RetryCountKey = "retrycount";

    /// <summary>
    /// Starts the consumer span for a received message, following the OpenTelemetry messaging
    /// conventions (<c>process {topic}</c>): a child of the trace context propagated with it, or the
    /// root of a new trace when the message carries none (or an unparsable one). Never a child of
    /// whatever span happens to be current on the delivering thread: a message is its own unit of
    /// work. It is linked to <paramref name="run"/>, the span of the run that consumed it, when
    /// there is one, so the run's messages can be found from it and vice versa; and to
    /// <paramref name="delivery"/>, the sidecar's span delivering it, so the Dapr hop (its retries
    /// and resiliency delays) can be followed. Every attribute is set at start, where samplers see it.
    /// </summary>
    /// <param name="message">The received message.</param>
    /// <param name="componentName">The component consuming it.</param>
    /// <param name="run">The span of the run consuming it; <see langword="default"/> for none.</param>
    /// <param name="delivery">The trace context the sidecar delivered it with (gRPC metadata);
    /// <see langword="default"/> for none.</param>
    internal static Activity? StartProcessActivity(IncomingMessage message, string componentName,
        ActivityContext run = default, ActivityContext delivery = default)
    {
        var parentContext = TryParseTraceContext(message.GetExtension(TraceParentKey),
            message.GetExtension(TraceStateKey), out var propagated)
            ? propagated
            : default;

        // With a default parent, StartActivity falls back to Activity.Current: clear it so a
        // message without trace context starts a new trace. The caller's context is async-local
        // to the message handler, so this does not leak past it.
        Activity.Current = null;

        var tags = new TagList
        {
            { MessagingAttributes.System, MessagingAttributes.DaprSystem },
            { MessagingAttributes.OperationType, MessagingAttributes.Process },
            { MessagingAttributes.OperationName, MessagingAttributes.Process },
            { MessagingAttributes.DestinationName, message.TopicName },
            { MessagingAttributes.MessageId, message.MessageId },
            { MessagingAttributes.MessageBodySize, message.Data.Length },
            { MessagingAttributes.PubSubName, message.PubSubName },
            { MessagingAttributes.ComponentName, componentName },
            { MessagingAttributes.EventId, message.MessageId }
        };
        AddIfPresent(ref tags, MessagingAttributes.EventSource, message.Source);
        AddIfPresent(ref tags, MessagingAttributes.EventSpecVersion, message.SpecVersion);
        AddIfPresent(ref tags, MessagingAttributes.EventType, message.Type);
        AddIfPresent(ref tags, MessagingAttributes.EventSubject, message.Subject);
        if (TryGetRetryCount(message.Extensions, out var retryCount))
            tags.Add(MessagingAttributes.MessageRetryCount, retryCount);

        return ActivitySourceProvider.ActivitySource.StartActivity(
            $"{MessagingAttributes.Process} {message.TopicName}",
            ActivityKind.Consumer,
            parentContext,
            tags,
            Links(parentContext, run, delivery));
    }

    /// <summary>Parses a W3C trace context, as propagated in a CloudEvent's extensions or a call's
    /// metadata.</summary>
    internal static bool TryParseTraceContext(string? traceParent, string? traceState, out ActivityContext context)
    {
        context = default;
        return !string.IsNullOrEmpty(traceParent) &&
               ActivityContext.TryParse(traceParent, traceState, isRemote: true, out context);
    }

    private static List<ActivityLink>? Links(ActivityContext parent, ActivityContext run, ActivityContext delivery)
    {
        var links = new List<ActivityLink>(2);
        if (run != default)
            links.Add(Link(run, "run"));

        // The sidecar may deliver with the very context the message carries: then it is the parent.
        if (delivery != default && delivery.SpanId != parent.SpanId)
            links.Add(Link(delivery, "delivery"));
        return links.Count == 0 ? null : links;
    }

    private static ActivityLink Link(ActivityContext context, string kind) =>
        new(context, new ActivityTagsCollection { [MessagingAttributes.LinkKind] = kind });

    private static void AddIfPresent(ref TagList tags, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value))
            tags.Add(key, value);
    }

    private static bool TryGetRetryCount(IReadOnlyDictionary<string, Value> extensions, out long retryCount)
    {
        retryCount = 0;
        if (!extensions.TryGetValue(RetryCountKey, out var value))
            return false;

        switch (value.KindCase)
        {
            case Value.KindOneofCase.NumberValue:
                retryCount = (long)value.NumberValue;
                return true;
            case Value.KindOneofCase.StringValue:
                return long.TryParse(value.StringValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out retryCount);
            default:
                return false;
        }
    }
}
