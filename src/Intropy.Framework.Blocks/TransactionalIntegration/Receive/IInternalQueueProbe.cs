namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive;

/// <summary>
/// An enqueuer whose internal queue can be probed: it publishes a <see cref="InternalQueueMessageTypes.Probe"/>
/// message to the same destination it publishes source files to. A Transactional Integration's
/// host publishes probes before it sweeps, until one comes back on the send side, because a
/// broker may accept a publish that no queue stores (RabbitMQ discards a message no queue is bound
/// for). An enqueuer that does not implement it is not probed.
/// </summary>
public interface IInternalQueueProbe
{
    /// <summary>Publishes one probe.</summary>
    /// <param name="probeId">The probe's CloudEvent id.</param>
    /// <param name="ct">Cancels the publish.</param>
    Task PublishProbeAsync(string probeId, CancellationToken ct);
}
