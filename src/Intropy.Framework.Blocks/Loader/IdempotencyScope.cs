namespace Intropy.Framework.Blocks.Loader;

/// <summary>
/// Which idempotency records a route of a routing loader is checked against. Only meaningful when
/// the loader routes several CloudEvent types; a loader without routes is keyed on the subject.
/// </summary>
public enum IdempotencyScope
{
    /// <summary>The route's own records: the id is <c>{event type}:{subject}</c>, so events of
    /// other types for the same subject never make this route's events look stale or duplicate.</summary>
    Route,

    /// <summary>Records shared by every route with this scope: the id is the subject, so an event
    /// older than one already processed for the same entity, of any type, is ignored. Use it when
    /// ordering across event types matters.</summary>
    Entity
}
