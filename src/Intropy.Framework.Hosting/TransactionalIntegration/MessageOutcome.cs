namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// How one consumed message ended in the run, as mapped by <see cref="MessageProcessor{TCtx}"/>
/// from the send pipeline's result.
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
    Interrupted
}
