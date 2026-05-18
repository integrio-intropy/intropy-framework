namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Dictionary keys used for checking and recording idempotency.
/// </summary>
internal static class IdempotencyContextKeys
{
    /// <summary>
    /// Context key for the unique identifier extracted from the payload.
    /// </summary>
    internal const string Id = "idempotency_id";

    /// <summary>
    /// Context key for the event date in ISO 8601 format.
    /// </summary>
    internal const string Date = "idempotency_date";

    /// <summary>
    /// Context key for the payload hash.
    /// </summary>
    internal const string Hash = "idempotency_hash";
}