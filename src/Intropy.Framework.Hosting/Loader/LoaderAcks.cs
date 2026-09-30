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
}
