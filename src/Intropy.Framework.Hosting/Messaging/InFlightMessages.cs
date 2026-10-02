using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// The messages a consumer is processing, and how it stops: once stopping begins it takes no new
/// message, gives those in flight a grace period to finish, then interrupts them so they are left
/// for redelivery. Also the idle clock: when the last message arrived, for consumers that stop once
/// their queue goes quiet.
/// </summary>
internal sealed class InFlightMessages : IDisposable
{
    /// <summary>After the grace period, interrupted messages get this long to return their ack.</summary>
    private static readonly TimeSpan s_interruptTimeout = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan s_pollInterval = TimeSpan.FromMilliseconds(50);

    private readonly CancellationTokenSource _interrupt = new();
    private readonly TimeProvider _time;
    private int _count;
    private long _lastActivityTicks;
    private volatile bool _stopping;

    internal InFlightMessages(TimeProvider? time = null)
    {
        _time = time ?? TimeProvider.System;
        RestartIdleClock();
    }

    /// <summary>Cancelled when messages still in flight after the grace period are interrupted.</summary>
    internal CancellationToken Interrupt => _interrupt.Token;

    /// <summary>How many messages are being processed.</summary>
    internal int Count => Volatile.Read(ref _count);

    /// <summary>Counts a message in, unless stopping has begun: then it must be left for the next
    /// consumer. A message counted in must be counted out with <see cref="Exit"/>.</summary>
    internal bool TryEnter()
    {
        if (_stopping)
            return false;

        Interlocked.Increment(ref _count);
        if (!_stopping)
        {
            RestartIdleClock();
            return true;
        }

        // Stopping began in between: the drain may already have seen no message in flight.
        Exit();
        return false;
    }

    /// <summary>Counts a message out.</summary>
    internal void Exit() => Interlocked.Decrement(ref _count);

    /// <summary>Restarts the idle clock, as if a message had just arrived.</summary>
    internal void RestartIdleClock() => Interlocked.Exchange(ref _lastActivityTicks, _time.GetUtcNow().UtcTicks);

    /// <summary>Whether no message is in flight and none arrived for <paramref name="idleTimeout"/>.</summary>
    internal bool IsIdle(TimeSpan idleTimeout) =>
        Count == 0 &&
        _time.GetUtcNow() - new DateTimeOffset(Interlocked.Read(ref _lastActivityTicks), TimeSpan.Zero) >= idleTimeout;

    /// <summary>Stops taking messages and waits for those in flight; interrupts them when they
    /// outlive <paramref name="gracePeriod"/>.</summary>
    /// <returns>How many messages were still in flight when the grace period ended.</returns>
    internal async Task<int> StopAsync(TimeSpan gracePeriod, ILogger logger)
    {
        _stopping = true;
        if (await WaitAsync(gracePeriod))
            return 0;

        var unfinished = Count;
        logger.LogWarning("{Count} message(s) still in flight after the shutdown grace period; interrupting them",
            unfinished);
        await _interrupt.CancelAsync();
        await WaitAsync(s_interruptTimeout);
        return unfinished;
    }

    public void Dispose() => _interrupt.Dispose();

    private async Task<bool> WaitAsync(TimeSpan timeout)
    {
        var deadline = _time.GetUtcNow() + timeout;
        while (Count > 0)
        {
            if (_time.GetUtcNow() >= deadline)
                return false;
            await Task.Delay(s_pollInterval, _time, CancellationToken.None);
        }

        return true;
    }
}
