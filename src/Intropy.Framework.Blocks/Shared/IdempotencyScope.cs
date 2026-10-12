namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Which idempotency records a route of a routing component is checked against: a loader that
/// routes several CloudEvent types, or an extractor that routes several source ports. A component
/// without routes is keyed on the id alone.
/// </summary>
public enum IdempotencyScope
{
    /// <summary>The route's own records. A loader route's id is <c>{event type}:{subject}</c>, so
    /// events of other types for the same subject never make this route's events look stale or
    /// duplicate. An extractor route's id is <c>{source port}:{id}</c>, so a record from another
    /// port with the same id never makes this port's file look like a duplicate.</summary>
    Route,

    /// <summary>Records shared by every route with this scope: the id alone. For a loader, an event
    /// older than one already processed for the same entity, of any type, is ignored; use it when
    /// ordering across event types matters. For an extractor, a record seen on one port is a
    /// duplicate on every other; use it for the same feed delivered in several places.</summary>
    Entity
}
