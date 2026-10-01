using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// The messages a loader is processing, and how it stops: once stopping begins it takes no new
/// message, gives those in flight a grace period to finish, then interrupts them so they are left
/// for redelivery. The same for every transport.
/// </summary>
internal sealed class InFlightMessages : IDisposable
{
    /// <summary>After the grace period, interrupted messages get this long to return their ack.</summary>
    private static readonly TimeSpan s_interruptTimeout = TimeSpan.FromSeconds(5);

    private readonly CancellationTokenSource _interrupt = new();
    private int _count;
    private volatile bool _stopping;

    /// <summary>Cancelled when messages still in flight after the grace period are interrupted.</summary>
    internal CancellationToken Interrupt => _interrupt.Token;

    /// <summary>Counts a message in, unless stopping has begun: then it must be left for the next
    /// consumer. A message counted in must be counted out with <see cref="Exit"/>.</summary>
    internal bool TryEnter()
    {
        if (_stopping)
            return false;

        Interlocked.Increment(ref _count);
        if (!_stopping)
            return true;

        // Stopping began in between: the drain may already have seen no message in flight.
        Exit();
        return false;
    }

    /// <summary>Counts a message out.</summary>
    internal void Exit() => Interlocked.Decrement(ref _count);

    /// <summary>Stops taking messages and waits for those in flight; interrupts them when they
    /// outlive <paramref name="gracePeriod"/>.</summary>
    internal async Task StopAsync(TimeSpan gracePeriod, ILogger logger)
    {
        _stopping = true;
        if (await WaitAsync(gracePeriod))
            return;

        logger.LogWarning("{Count} message(s) still in flight after the shutdown grace period; interrupting them",
            Volatile.Read(ref _count));
        await _interrupt.CancelAsync();
        await WaitAsync(s_interruptTimeout);
    }

    public void Dispose() => _interrupt.Dispose();

    private async Task<bool> WaitAsync(TimeSpan timeout)
    {
        var elapsed = Stopwatch.StartNew();
        while (Volatile.Read(ref _count) > 0)
        {
            if (elapsed.Elapsed >= timeout)
                return false;
            await Task.Delay(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        }

        return true;
    }
}
