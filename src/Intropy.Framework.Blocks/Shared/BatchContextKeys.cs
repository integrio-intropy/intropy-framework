namespace Intropy.Framework.Blocks.Shared;

/// <summary>Context keys a batch loader writes for the per-entity steps.</summary>
public static class BatchContextKeys
{
    /// <summary>The entry's entity key (<c>KeyedBy</c>): the batch loader's coalescing and lookup
    /// key, and its idempotency id.</summary>
    public const string EntityKey = "intropy.entity_key";
}
