namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// What handling one request amounted to: the decision the gateway answers its caller with. The
/// caller is the gateway's only retry mechanism — no item stays behind for a next run — so each
/// outcome tells the caller whether to send the request again.
/// </summary>
/// <param name="RequestId">The request's identity (<c>X-Request-Id</c>), echoed to the caller.</param>
public abstract record GatewayOutcome(string RequestId)
{
    /// <summary>The item was published: durably handed to the pub/sub. Do not resend.</summary>
    /// <param name="RequestId">The request's identity.</param>
    /// <param name="EventId">The id of the CloudEvent the gateway published.</param>
    public sealed record Published(string RequestId, string EventId) : GatewayOutcome(RequestId);

    /// <summary>The item was published before, unchanged (the idempotency check). Safe to resend;
    /// nothing is published again.</summary>
    /// <param name="RequestId">The request's identity.</param>
    public sealed record Duplicate(string RequestId) : GatewayOutcome(RequestId);

    /// <summary>The item failed a business rule and was routed to the Business Incident Service.
    /// Resending it unchanged fails again.</summary>
    /// <param name="RequestId">The request's identity.</param>
    /// <param name="Description">Why the item was rejected, when the failing step said.</param>
    public sealed record Rejected(string RequestId, string? Description) : GatewayOutcome(RequestId);

    /// <summary>The body exceeded <see cref="GatewayOptions.MaxRequestBodyBytes"/>; no step ran.</summary>
    /// <param name="RequestId">The request's identity.</param>
    /// <param name="Limit">The largest body the gateway accepts, in bytes.</param>
    public sealed record TooLarge(string RequestId, long Limit) : GatewayOutcome(RequestId);

    /// <summary>The gateway could not take the item now — it is not ready, or its sidecar, the
    /// pub/sub or a platform service is unavailable. Nothing was published; resend after
    /// <paramref name="RetryAfter"/>.</summary>
    /// <param name="RequestId">The request's identity.</param>
    /// <param name="Description">What failed.</param>
    /// <param name="RetryAfter">How long the caller should wait before resending.</param>
    public sealed record Unavailable(string RequestId, string Description, TimeSpan RetryAfter)
        : GatewayOutcome(RequestId);

    /// <summary>A step failed technically in a way a resend cannot cure — a fault in the
    /// component's code or configuration, such as an output event without a subject. Nothing was
    /// published. Do not resend unchanged: the failure needs a fix in the gateway.</summary>
    /// <param name="RequestId">The request's identity.</param>
    /// <param name="Description">What failed.</param>
    public sealed record Failed(string RequestId, string Description) : GatewayOutcome(RequestId);
}
