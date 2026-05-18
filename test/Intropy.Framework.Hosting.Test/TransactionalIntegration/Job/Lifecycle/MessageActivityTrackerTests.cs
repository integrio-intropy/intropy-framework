using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Job.Lifecycle;

public class MessageActivityTrackerTests
{
    private readonly ILogger _logger = Substitute.For<ILogger>();

    [Fact]
    public void ShouldShutdownDueToInactivity_ReturnsFalse_WhenMessagesAreInProgress()
    {
        // Verifies that shutdown is prevented while messages are still being processed
        var tracker = new MessageActivityTracker();
        var scope = tracker.BeginMessageProcessing();

        var shouldShutdown = tracker.ShouldShutdownDueToInactivity(TimeSpan.FromSeconds(1));

        Assert.False(shouldShutdown);
        scope.Dispose();
    }

    [Fact]
    public async Task ShouldShutdownDueToInactivity_ReturnsFalse_BeforeIdleTimeoutReached()
    {
        // Verifies that shutdown doesn't trigger prematurely before the idle timeout period
        var tracker = new MessageActivityTracker();

        await Task.Delay(100);

        var shouldShutdown = tracker.ShouldShutdownDueToInactivity(TimeSpan.FromSeconds(10));

        Assert.False(shouldShutdown);
    }

    [Fact]
    public async Task ShouldShutdownDueToInactivity_ReturnsTrue_WhenIdleTimeoutReachedAndNoMessagesInProgress()
    {
        // Verifies that shutdown is triggered when idle timeout is reached and no messages are processing
        var tracker = new MessageActivityTracker();

        await Task.Delay(150);

        var shouldShutdown = tracker.ShouldShutdownDueToInactivity(TimeSpan.FromMilliseconds(100));

        Assert.True(shouldShutdown);
    }

    [Fact]
    public async Task ShouldShutdownDueToInactivity_ReturnsFalse_WhenNewMessageReceivedWithinTimeout()
    {
        // Verifies that receiving a new message resets the idle timer
        var tracker = new MessageActivityTracker();

        await Task.Delay(50);
        var scope = tracker.BeginMessageProcessing();
        scope.Dispose();
        await Task.Delay(50);

        var shouldShutdown = tracker.ShouldShutdownDueToInactivity(TimeSpan.FromMilliseconds(150));

        Assert.False(shouldShutdown);
    }

    [Fact]
    public async Task BeginMessageProcessing_ShouldTrackMultipleConcurrentMessages()
    {
        // Verifies that multiple concurrent messages are correctly tracked
        var tracker = new MessageActivityTracker();

        var scope1 = tracker.BeginMessageProcessing();
        var scope2 = tracker.BeginMessageProcessing();
        var scope3 = tracker.BeginMessageProcessing();

        var shouldShutdown = tracker.ShouldShutdownDueToInactivity(TimeSpan.Zero);
        Assert.False(shouldShutdown);

        scope1.Dispose();
        shouldShutdown = tracker.ShouldShutdownDueToInactivity(TimeSpan.Zero);
        Assert.False(shouldShutdown);

        scope2.Dispose();
        scope3.Dispose();

        await Task.Delay(10);
        shouldShutdown = tracker.ShouldShutdownDueToInactivity(TimeSpan.Zero);
        Assert.True(shouldShutdown);
    }

    [Fact]
    public async Task WaitForAllMessagesToComplete_ShouldReturnImmediately_WhenNoMessagesInProgress()
    {
        // Verifies that graceful shutdown completes immediately when no messages are being processed
        var tracker = new MessageActivityTracker();

        var startTime = DateTime.UtcNow;
        await tracker.WaitForAllMessagesToComplete(TimeSpan.FromSeconds(5), _logger);
        var elapsed = DateTime.UtcNow - startTime;

        Assert.True(elapsed < TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task WaitForAllMessagesToComplete_ShouldWaitForMessagesToComplete()
    {
        // Verifies that graceful shutdown waits for in-flight messages to finish processing
        var tracker = new MessageActivityTracker();
        var scope = tracker.BeginMessageProcessing();

        var waitTask = tracker.WaitForAllMessagesToComplete(TimeSpan.FromSeconds(5), _logger);

        await Task.Delay(100);
        Assert.False(waitTask.IsCompleted);

        scope.Dispose();
        await waitTask;

        Assert.True(waitTask.IsCompleted);
    }

    [Fact]
    public async Task WaitForAllMessagesToComplete_ShouldTimeout_WhenMessagesDoNotComplete()
    {
        // Verifies that graceful shutdown doesn't hang indefinitely if messages fail to complete
        var tracker = new MessageActivityTracker();
        var scope = tracker.BeginMessageProcessing();

        var startTime = DateTime.UtcNow;
        await tracker.WaitForAllMessagesToComplete(TimeSpan.FromMilliseconds(500), _logger);
        var elapsed = DateTime.UtcNow - startTime;

        Assert.True(elapsed >= TimeSpan.FromMilliseconds(500));
        Assert.True(elapsed < TimeSpan.FromSeconds(2));

        scope.Dispose();
    }

    [Fact]
    public void MessageProcessingScope_ShouldBeIdempotent_WhenDisposedMultipleTimes()
    {
        // Verifies that disposing a message scope multiple times doesn't cause errors or incorrect state
        var tracker = new MessageActivityTracker();
        var scope = tracker.BeginMessageProcessing();

        scope.Dispose();
        scope.Dispose();
        scope.Dispose();

        var shouldShutdown = tracker.ShouldShutdownDueToInactivity(TimeSpan.Zero);
        Assert.True(shouldShutdown);
    }
}
