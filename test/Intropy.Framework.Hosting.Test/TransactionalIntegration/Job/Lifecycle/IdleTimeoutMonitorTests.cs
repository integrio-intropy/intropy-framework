using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Job.Lifecycle;

public class IdleTimeoutMonitorTests
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(30);

    private readonly ILogger _logger = Substitute.For<ILogger>();
    private readonly FakeTimeProvider _time = new();

    [Fact]
    public async Task WaitForIdleTimeoutAsync_ShouldReturn_WhenIdleTimeoutReached()
    {
        // Verifies that the monitor triggers shutdown when no messages are received within the timeout period
        var tracker = new MessageActivityTracker(_time);
        var monitor = new IdleTimeoutMonitor(tracker, IdleTimeout, _logger, _time, PollInterval);

        var task = monitor.WaitForIdleTimeoutAsync(CancellationToken.None);

        _time.Advance(IdleTimeout + PollInterval);

        await task;
        Assert.True(task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task WaitForIdleTimeoutAsync_ShouldNotReturn_WhileMessagesAreBeingProcessed()
    {
        // Verifies that active message processing prevents shutdown
        var tracker = new MessageActivityTracker(_time);
        var monitor = new IdleTimeoutMonitor(tracker, IdleTimeout, _logger, _time, PollInterval);

        using var scope = tracker.BeginMessageProcessing();

        var task = monitor.WaitForIdleTimeoutAsync(CancellationToken.None);

        // Advance well past the idle timeout. Polls should fire, but each one sees
        // the in-progress message and keeps waiting.
        _time.Advance(IdleTimeout * 3);
        await Task.Yield();

        Assert.False(task.IsCompleted);
    }

    [Fact]
    public async Task WaitForIdleTimeoutAsync_ShouldRespectCancellationToken()
    {
        // Verifies that the monitor can be cancelled externally for emergency shutdown
        var tracker = new MessageActivityTracker(_time);
        var monitor = new IdleTimeoutMonitor(tracker, IdleTimeout, _logger, _time, PollInterval);

        using var cts = new CancellationTokenSource();
        var task = monitor.WaitForIdleTimeoutAsync(cts.Token);

        await cts.CancelAsync();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await task);
    }

    [Fact]
    public async Task WaitForIdleTimeoutAsync_ShouldWaitForNewIdlePeriod_WhenMessageReceivedDuringMonitoring()
    {
        // Verifies that receiving new messages resets the idle timeout timer
        var tracker = new MessageActivityTracker(_time);
        var monitor = new IdleTimeoutMonitor(tracker, IdleTimeout, _logger, _time, PollInterval);

        var task = monitor.WaitForIdleTimeoutAsync(CancellationToken.None);

        // Almost-but-not-quite hit the idle timeout, then simulate message activity.
        _time.Advance(IdleTimeout - PollInterval);
        using (tracker.BeginMessageProcessing()) { }
        await Task.Yield();
        Assert.False(task.IsCompleted);

        // Advancing again by less than the full idle timeout (measured from the
        // most recent message) must not trigger shutdown.
        _time.Advance(IdleTimeout - PollInterval);
        await Task.Yield();
        Assert.False(task.IsCompleted);

        // Now advance past the new idle window — shutdown should fire.
        _time.Advance(IdleTimeout);
        await task;
        Assert.True(task.IsCompletedSuccessfully);
    }
}
