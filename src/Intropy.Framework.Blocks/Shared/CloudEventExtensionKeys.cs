namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// The CloudEvent extension attributes the framework puts on the wire and reads back: the contract
/// between every publisher (the blocks) and every subscriber (the hosting templates), kept in one
/// place so neither side can drift from the other.
/// </summary>
public static class CloudEventExtensionKeys
{
    /// <summary>The pipeline context metadata, as a JSON object of string keys and values,
    /// propagated with each message so the consumer rebuilds the context it was sent with.</summary>
    public const string Metadata = "metadata";
}
