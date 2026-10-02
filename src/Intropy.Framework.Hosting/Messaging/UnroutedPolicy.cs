namespace Intropy.Framework.Hosting.Messaging;

/// <summary>What a routing loader does with a message whose CloudEvent type no route handles.</summary>
public enum UnroutedPolicy
{
    /// <summary>Leave the message for redelivery, so it ends in the broker's dead-letter queue (the
    /// default): at once without a Dapr <c>Resiliency</c> retry policy, after its retries with one.
    /// Visible, and replayable from there.</summary>
    DeadLetter,

    /// <summary>Acknowledge and drop the message, with a warning log. Use when the topic carries
    /// event types this loader deliberately ignores.</summary>
    Ack
}
