using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Job.Lifecycle;

public class IdleTimeoutMonitorTests
{
    private readonly ILogger _logger = Substitute.For<ILogger>();

    [Fact]
    public async Task WaitForIdleTimeoutAsync_ShouldReturn_WhenIdleTimeoutReached()
    {
        // Verifies that the monitor triggers shutdown when no messages are received within the timeout period
        var tracker = new MessageActivityTracker();
        var monitor = new IdleTimeoutMonitor(tracker, TimeSpan.FromMilliseconds(100), _logger);

        var startTime = DateTime.UtcNow;
        await monitor.WaitForIdleTimeoutAsync(CancellationToken.None);
        var elapsed = DateTime.UtcNow - startTime;

        Assert.True(elapsed >= TimeSpan.FromMilliseconds(100));
        Assert.True(elapsed < TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task WaitForIdleTimeoutAsync_ShouldNotReturn_WhileMessagesAreBeingProcessed()
    {
        // Verifies that active message processing prevents shutdown
        var tracker = new MessageActivityTracker();
        var monitor = new IdleTimeoutMonitor(tracker, TimeSpan.FromMilliseconds(100), _logger);

        var scope = tracker.BeginMessageProcessing();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var task = monitor.WaitForIdleTimeoutAsync(cts.Token);

        await Task.Delay(200, CancellationToken.None);
        Assert.False(task.IsCompleted);

        scope.Dispose();
    }

    [Fact]
    public async Task WaitForIdleTimeoutAsync_ShouldRespectCancellationToken()
    {
        // Verifies that the monitor can be cancelled externally for emergency shutdown
        var tracker = new MessageActivityTracker();
        var monitor = new IdleTimeoutMonitor(tracker, TimeSpan.FromSeconds(100), _logger);

        using var cts = new CancellationTokenSource();
        var task = monitor.WaitForIdleTimeoutAsync(cts.Token);

        await Task.Delay(50, CancellationToken.None);
        await cts.CancelAsync();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await task);
    }

    [Fact]
    public async Task WaitForIdleTimeoutAsync_ShouldWaitForNewIdlePeriod_WhenMessageReceivedDuringMonitoring()
    {
        // Verifies that receiving new messages resets the idle timeout timer
        var tracker = new MessageActivityTracker();
        var monitor = new IdleTimeoutMonitor(tracker, TimeSpan.FromMilliseconds(500), _logger);

        var monitorTask = Task.Run(async () => await monitor.WaitForIdleTimeoutAsync(CancellationToken.None));

        // Wait a bit, then simulate message activity which should reset the timer
        await Task.Delay(200);
        var scope = tracker.BeginMessageProcessing();
        scope.Dispose();

        // Shortly after message activity, task should not be completed yet
        await Task.Delay(300);
        Assert.False(monitorTask.IsCompleted);

        // Wait for full idle timeout after the message activity
        await Task.Delay(550);
        Assert.True(monitorTask.IsCompleted);
    }
}
