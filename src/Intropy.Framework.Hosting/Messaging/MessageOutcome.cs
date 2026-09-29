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

    /// <summary>Interrupted by the host stopping: left for redelivery, but not counted.</summary>
    Interrupted,

    /// <summary>No route handles the message's event type; acknowledged as the loader's
    /// <see cref="UnroutedPolicy"/> says.</summary>
    Unrouted,

    /// <summary>Excluded by a batch route's filter: acknowledged without being looked up or sent.</summary>
    Filtered
}
