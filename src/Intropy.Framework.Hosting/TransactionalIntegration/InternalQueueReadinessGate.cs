using System.Collections.Concurrent;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// Holds the sweep back until the internal queue delivers. A broker can accept a publish that no
/// queue stores: RabbitMQ discards a message published before the subscription has declared and
/// bound its queue, as on a first deploy or after the queue was deleted. A file completed after
/// such a publish is lost. So before the sweep, the gate publishes probes through the receive
/// side's enqueuer until one of this run's probes reaches the send side. An old queued message
/// proves consumption works, not that the current publisher reaches the queue. This is a startup
/// check, not a guarantee against the queue disappearing later. Only one job instance should
/// consume the internal queue at a time; competing consumers can receive each other's probes.
/// </summary>
/// <param name="probe">The receive side's enqueuer, which publishes the probes.</param>
/// <param name="readyTimeout">How long to wait for a delivery before giving up on the run.</param>
/// <param name="probeInterval">How long to wait for a probe before publishing another: earlier
/// probes may have been discarded for the very reason the gate exists.</param>
/// <param name="logger">Logs the probing.</param>
/// <param name="timeProvider">Time source for the waits. Defaults to <see cref="TimeProvider.System"/>.</param>
internal sealed class InternalQueueReadinessGate(
    IInternalQueueProbe probe,
    TimeSpan readyTimeout,
    TimeSpan probeInterval,
    ILogger logger,
    TimeProvider? timeProvider = null)
{
    private readonly IInternalQueueProbe _probe = probe;
    private readonly TimeSpan _readyTimeout = readyTimeout;
    private readonly TimeSpan _probeInterval = probeInterval;
    private readonly ILogger _logger = logger;
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <summary>Creates one run's readiness check without publishing: hand
    /// <see cref="ReadinessCheck.Delivered"/> to the send side to report probe ids, and call
    /// <see cref="ReadinessCheck.WaitAsync"/> to begin probing before sweeping.</summary>
    internal ReadinessCheck CreateCheck() => new(this);

    /// <summary>One run's internal queue readiness check.</summary>
    internal sealed class ReadinessCheck
    {
        private readonly InternalQueueReadinessGate _gate;
        private readonly TaskCompletionSource _delivered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ConcurrentDictionary<string, byte> _probeIds = new(StringComparer.Ordinal);

        internal ReadinessCheck(InternalQueueReadinessGate gate) => _gate = gate;

        /// <summary>Records delivery only for a probe issued by this run. Stale probes cannot
        /// make a new run ready.</summary>
        internal void Delivered(string probeId)
        {
            if (_probeIds.ContainsKey(probeId))
                _delivered.TrySetResult();
        }

        /// <summary>Publishes probes until one issued by this run reaches the send side.</summary>
        /// <exception cref="InfrastructureUnavailableException">No matching probe within the ready
        /// timeout: the run must not sweep.</exception>
        internal async Task WaitAsync(CancellationToken ct)
        {
            using var deadline = new CancellationTokenSource(_gate._readyTimeout, _gate._time);
            using var wait = CancellationTokenSource.CreateLinkedTokenSource(ct, deadline.Token);
            var probes = 0;
            try
            {
                while (!_delivered.Task.IsCompleted)
                {
                    // Register before publishing: delivery can race the publish response. Use
                    // fresh ids on retries so broker duplicate detection cannot suppress them.
                    var probeId = Guid.NewGuid().ToString();
                    _probeIds.TryAdd(probeId, 0);
                    try
                    {
                        await _gate._probe.PublishProbeAsync(probeId, wait.Token);
                        probes++;
                        _gate._logger.LogDebug("Published internal queue probe {ProbeId}", probeId);
                    }
                    catch (Exception e) when (e is not OperationCanceledException)
                    {
                        _gate._logger.LogWarning(e, "Publishing internal queue probe {ProbeId} failed; retrying", probeId);
                    }

                    try
                    {
                        await _delivered.Task.WaitAsync(_gate._probeInterval, _gate._time, wait.Token);
                    }
                    catch (TimeoutException)
                    {
                        // Not back yet: publish another.
                    }
                }
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested && deadline.IsCancellationRequested)
            {
                throw new InfrastructureUnavailableException(
                    $"No probe from this run reached the internal queue within {_gate._readyTimeout.TotalSeconds} seconds " +
                    $"({probes} probe(s) published): the subscription may not be live, or its queue may not " +
                    "exist. No source file was touched.");
            }

            ct.ThrowIfCancellationRequested();
            _gate._logger.LogInformation("A probe from this run reached the internal queue; starting the sweep");
        }
    }
}
