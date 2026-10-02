namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// The attribute names a consumed message's spans, metrics and log scopes carry: the OpenTelemetry
/// messaging, CloudEvents and error conventions where they define one, the <c>intropy.</c>
/// namespace where they do not.
/// </summary>
internal static class MessagingAttributes
{
    internal const string System = "messaging.system";
    internal const string OperationType = "messaging.operation.type";
    internal const string OperationName = "messaging.operation.name";
    internal const string DestinationName = "messaging.destination.name";
    internal const string MessageId = "messaging.message.id";
    internal const string MessageBodySize = "messaging.message.body.size";

    internal const string EventId = "cloudevents.event_id";
    internal const string EventSource = "cloudevents.event_source";
    internal const string EventSpecVersion = "cloudevents.event_spec_version";
    internal const string EventType = "cloudevents.event_type";
    internal const string EventSubject = "cloudevents.event_subject";

    internal const string ErrorType = "error.type";

    /// <summary>The Dapr pub/sub component the message was delivered from: with the topic, the
    /// destination's full address. The messaging conventions have no attribute for it.</summary>
    internal const string PubSubName = "intropy.pubsub.name";

    internal const string ComponentName = "intropy.component.name";

    /// <summary>The loader route that handled the message.</summary>
    internal const string LoaderRoute = "intropy.loader.route";

    /// <summary>How the message ended: <c>processed</c>, <c>skipped</c>, <c>failed</c>,
    /// <c>interrupted</c>, <c>unrouted</c> or <c>rejected</c>.</summary>
    internal const string MessageOutcome = "intropy.message.outcome";

    /// <summary>The <c>retrycount</c> CloudEvent extension of a redelivered message. The messaging
    /// conventions only have broker-specific delivery counts.</summary>
    internal const string MessageRetryCount = "intropy.message.retry_count";

    /// <summary>Why a span links to another: <c>run</c> for the run that consumed the message,
    /// <c>delivery</c> for the sidecar's delivery of it.</summary>
    internal const string LinkKind = "intropy.link.kind";

    /// <summary>The value of <see cref="System"/>: messages arrive through Dapr pub/sub, whatever
    /// broker backs it.</summary>
    internal const string DaprSystem = "dapr";

    /// <summary>The value of <see cref="OperationType"/> and <see cref="OperationName"/>.</summary>
    internal const string Process = "process";
}
