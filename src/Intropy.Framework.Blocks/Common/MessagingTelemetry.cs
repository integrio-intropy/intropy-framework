using System.Diagnostics;
using CloudNative.CloudEvents;

namespace Intropy.Framework.Blocks.Common;

/// <summary>
/// The send side of a queue hop, following the OpenTelemetry messaging conventions: a
/// <c>send {destination}</c> producer span whose context the CloudEvent carries (the CloudEvents
/// distributed tracing extension), so the consumer's span continues it.
/// </summary>
internal static class MessagingTelemetry
{
    private const string TraceParentKey = "traceparent";
    private const string TraceStateKey = "tracestate";

    /// <summary>Starts the producer span for publishing one message.</summary>
    /// <param name="destination">The destination, such as the topic; <see langword="null"/> when unknown.</param>
    /// <param name="system">The messaging system (<c>messaging.system</c>); <see langword="null"/> when unknown.</param>
    internal static Activity? StartSendActivity(string? destination, string? system)
    {
        var activity = ActivitySourceProvider.ActivitySource.StartActivity(
            destination is null ? "send" : $"send {destination}", ActivityKind.Producer);
        if (activity is null)
            return null;

        activity.SetTag("messaging.operation.type", "send");
        activity.SetTag("messaging.operation.name", "send");
        if (system is not null)
            activity.SetTag("messaging.system", system);
        if (destination is not null)
            activity.SetTag("messaging.destination.name", destination);
        return activity;
    }

    /// <summary>Marks the send as failed.</summary>
    internal static void Fail(Activity? activity, string errorType, string description)
    {
        activity?.SetTag("error.type", errorType);
        activity?.SetStatus(ActivityStatusCode.Error, description);
    }

    /// <summary>Marks the send as failed with <paramref name="exception"/>.</summary>
    internal static void Fail(Activity? activity, Exception exception)
    {
        activity?.AddException(exception);
        Fail(activity, exception.GetType().FullName!, exception.Message);
    }

    /// <summary>Puts <paramref name="activity"/>'s W3C trace context on <paramref name="cloudEvent"/>,
    /// replacing any it carried.</summary>
    internal static CloudEvent Propagate(Activity? activity, CloudEvent cloudEvent)
    {
        if (activity == null)
            return cloudEvent;

        if (!string.IsNullOrEmpty(activity.Id))
        {
            var attr = CloudEventAttribute.CreateExtension(TraceParentKey, CloudEventAttributeType.String);
            cloudEvent[attr] = activity.Id;
        }

        if (!string.IsNullOrEmpty(activity.TraceStateString))
        {
            var attr = CloudEventAttribute.CreateExtension(TraceStateKey, CloudEventAttributeType.String);
            cloudEvent[attr] = activity.TraceStateString;
        }

        return cloudEvent;
    }
}
