namespace Intropy.Framework.Hosting.Messaging;

/// <summary>What a routing loader does with a message whose CloudEvent type no route handles.</summary>
public enum UnroutedPolicy
{
    /// <summary>Hand the message to the subscription's dead-letter topic (the default). Visible, and
    /// never redelivered in a loop. Requires <c>LoaderOptions.DeadLetterTopic</c>.</summary>
    DeadLetter,

    /// <summary>Acknowledge and drop the message, with a warning log. Use when the topic carries
    /// event types this loader deliberately ignores.</summary>
    Ack,

    /// <summary>Leave the message for redelivery: for a rollout where another version of the loader
    /// handles the type. Loops until one does.</summary>
    Retry
}
