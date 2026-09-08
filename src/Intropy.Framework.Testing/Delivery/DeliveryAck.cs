namespace Intropy.Framework.Testing.Delivery;

/// <summary>
/// The ack a Dapr subscription endpoint returns to the sidecar in the <c>{"status": "..."}</c>
/// response body.
/// </summary>
/// <remarks>
/// Members map to Dapr's wire values as follows: <see cref="Success"/> ↔ <c>SUCCESS</c>,
/// <see cref="Retry"/> ↔ <c>RETRY</c>, <see cref="Drop"/> ↔ <c>DROP</c>. Unknown, missing, or
/// malformed wire values map to <see cref="Retry"/> — matching the sidecar, which redelivers on any
/// status it does not recognize (fail-safe).
/// </remarks>
public enum DeliveryAck
{
    /// <summary>The message is consumed; the sidecar will not redeliver. Wire value: <c>SUCCESS</c>.</summary>
    Success,

    /// <summary>The sidecar should redeliver the message. Wire value: <c>RETRY</c>.</summary>
    Retry,

    /// <summary>The message is dropped without further processing. Wire value: <c>DROP</c>.</summary>
    Drop,
}
