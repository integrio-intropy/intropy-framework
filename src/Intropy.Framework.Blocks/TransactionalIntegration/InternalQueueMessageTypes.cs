namespace Intropy.Framework.Blocks.TransactionalIntegration;

/// <summary>
/// The CloudEvent types a Transactional Integration stamps on the messages it publishes to its
/// own internal queue. The receive side publishes them and the send side recognises them, so both
/// read this one definition.
/// </summary>
public static class InternalQueueMessageTypes
{
    /// <summary>A received source file, published for the integration's own send side.</summary>
    public const string Received = "transactional-integration.received";

    /// <summary>A probe published before the sweep: delivery of a probe from the current run
    /// proves the publish path reaches the send side at startup. It does not protect against the
    /// queue disappearing later. It carries no data and never runs through the send pipeline.</summary>
    public const string Probe = "transactional-integration.probe";
}
