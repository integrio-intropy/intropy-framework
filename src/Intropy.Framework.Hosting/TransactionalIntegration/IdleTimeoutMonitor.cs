using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// Monitors message activity and determines when the system should shut down due to inactivity.
/// </summary>
internal class IdleTimeoutMonitor
{
    private readonly InFlightMessages _inFlight;
    private readonly TimeSpan _idleTimeout;
    private readonly ILogger _logger;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _pollInterval;

    /// <summary>
    /// Creates a new <see cref="IdleTimeoutMonitor"/>.
    /// </summary>
    /// <param name="inFlight">The messages in flight and the idle clock to monitor.</param>
    /// <param name="idleTimeout">The duration of inactivity before triggering shutdown.</param>
    /// <param name="logger">An instance of <see cref="ILogger"/> for logging.</param>
    /// <param name="timeProvider">Time source. Defaults to <see cref="TimeProvider.System"/>. Override in tests to control time.</param>
    /// <param name="pollInterval">How often inactivity is checked. Defaults to 1 second.</param>
    public IdleTimeoutMonitor(
        InFlightMessages inFlight,
        TimeSpan idleTimeout,
        ILogger logger,
        TimeProvider? timeProvider = null,
        TimeSpan? pollInterval = null)
    {
        _inFlight = inFlight;
        _idleTimeout = idleTimeout;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _pollInterval = pollInterval ?? TimeSpan.FromSeconds(1);
    }

    /// <summary>
    /// Monitors for idle timeout and returns when shutdown should begin.
    /// This method blocks until the idle timeout condition is met.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the monitoring.</param>
    public async Task WaitForIdleTimeoutAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(_pollInterval, _timeProvider, cancellationToken);

            if (!_inFlight.IsIdle(_idleTimeout))
                continue;

            _logger.LogInformation(
                "No messages received for {TimeoutSeconds}s and no messages in progress. Shutting down.",
                _idleTimeout.TotalSeconds);
            return;
        }
    }
}
