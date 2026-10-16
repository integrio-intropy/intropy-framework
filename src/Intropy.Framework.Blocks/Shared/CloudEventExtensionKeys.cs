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

    /// <summary>The source port the message's file was swept from: on a Transactional
    /// Integration's internal messages, where the send side routes on it, and on the events of an
    /// extractor that routes several source ports, where consumers can filter on it. Set by the
    /// framework from the sweep, never from the context metadata, so a step cannot change it.</summary>
    public const string SourcePort = "intropysourceport";

    /// <summary>The claim-check pointer: the path, inside the component's claim store, of a payload
    /// too large to ride inside the envelope. Which store the path names is configuration on both the
    /// publishing and the consuming side, never part of the event. When it is set the envelope carries
    /// no <c>data</c>; a consumer resolves the path back to the payload before the pipeline sees it.
    /// The standard CloudEvents <c>dataref</c> extension, so the envelope stays spec-compliant.</summary>
    public const string DataRef = "dataref";
}
