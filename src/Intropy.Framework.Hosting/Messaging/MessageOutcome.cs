namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// How one consumed message ended, whatever consumed it (a Transactional Integration's send
/// pipeline or a loader route). The transport maps it to its acknowledgement.
/// </summary>
internal enum MessageOutcome
{
    /// <summary>Acknowledged: the message was processed.</summary>
    Processed,

    /// <summary>An idempotent duplicate: acknowledged, counted as skipped.</summary>
    Skipped,

    /// <summary>Left for redelivery, counted as failed.</summary>
    Failed,

    /// <summary>Interrupted by the host stopping: left for redelivery. Recorded as
    /// <c>interrupted</c>, not as a failure, and left out of a Transactional Integration's run
    /// summary.</summary>
    Interrupted,

    /// <summary>No route handles the message's event type; acknowledged as the loader's
    /// <see cref="UnroutedPolicy"/> says.</summary>
    Unrouted
}
