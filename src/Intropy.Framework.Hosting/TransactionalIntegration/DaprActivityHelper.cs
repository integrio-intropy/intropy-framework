using System.Diagnostics;
using System.Globalization;
using Dapr.Messaging.PublishSubscribe;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Common;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

internal static class DaprActivityHelper
{
    private const string TraceParentKey = "traceparent";
    private const string TraceStateKey = "tracestate";
    private const string RetryCountKey = "retrycount";

    /// <summary>
    /// Starts the consumer span for a received message, following the OpenTelemetry messaging
    /// conventions (<c>process {topic}</c>): a child of the trace context propagated with it, or the
    /// root of a new trace when the message carries none (or an unparsable one). Never a child of
    /// whatever span happens to be current on the delivering thread, such as the job's: a message
    /// is its own unit of work. It is linked to <paramref name="run"/>, the span of the run that
    /// consumed it, so the run's messages can be found from it and vice versa.
    /// </summary>
    internal static Activity? StartProcessActivity(TopicMessage message, string topic, ActivityContext run)
    {
        var extensions = message.Extensions;
        var traceParent = extensions.TryGetValue(TraceParentKey, out var parentValue)
            ? parentValue.StringValue
            : null;
        var traceState = extensions.TryGetValue(TraceStateKey, out var stateValue)
            ? stateValue.StringValue
            : null;

        if (string.IsNullOrEmpty(traceParent) ||
            !ActivityContext.TryParse(traceParent, traceState, isRemote: true, out var parentContext))
            parentContext = default;

        // With a default parent, StartActivity falls back to Activity.Current: clear it so a
        // message without trace context starts a new trace. The caller's context is async-local
        // to the message handler, so this does not leak past it.
        Activity.Current = null;

        var activity = ActivitySourceProvider.ActivitySource.StartActivity(
            $"process {topic}",
            ActivityKind.Consumer,
            parentContext,
            links: run == default ? null : [new ActivityLink(run)]);
        if (activity is null)
            return null;

        activity.SetTag("messaging.system", "dapr");
        activity.SetTag("messaging.operation.type", "process");
        activity.SetTag("messaging.operation.name", "process");
        activity.SetTag("messaging.destination.name", topic);
        activity.SetTag("messaging.message.id", message.Id);
        if (TryGetRetryCount(extensions, out var retryCount))
            activity.SetTag("intropy.message.retry_count", retryCount);
        return activity;
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
