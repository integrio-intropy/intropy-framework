using Dapr.AppCallback.Autogen.Grpc.v1;
using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>A loader's acknowledgement of a message, from its outcome — the same for every transport.</summary>
internal static class LoaderAcks
{
    internal static TopicResponseAction ToResponse(MessageOutcome outcome, UnroutedPolicy unrouted) => outcome switch
    {
        MessageOutcome.Processed or MessageOutcome.Skipped => TopicResponseAction.Success,
        MessageOutcome.Unrouted => unrouted switch
        {
            // Drop hands the message to the subscription's dead-letter topic.
            UnroutedPolicy.DeadLetter => TopicResponseAction.Drop,
            UnroutedPolicy.Ack => TopicResponseAction.Success,
            _ => TopicResponseAction.Retry
        },
        _ => TopicResponseAction.Retry
    };

    /// <summary>The same ack, as the app callback's response status.</summary>
    internal static TopicEventResponse.Types.TopicEventResponseStatus ToStatus(MessageOutcome outcome,
        UnroutedPolicy unrouted) => ToResponse(outcome, unrouted) switch
    {
        TopicResponseAction.Success => TopicEventResponse.Types.TopicEventResponseStatus.Success,
        TopicResponseAction.Drop => TopicEventResponse.Types.TopicEventResponseStatus.Drop,
        _ => TopicEventResponse.Types.TopicEventResponseStatus.Retry
    };
}
