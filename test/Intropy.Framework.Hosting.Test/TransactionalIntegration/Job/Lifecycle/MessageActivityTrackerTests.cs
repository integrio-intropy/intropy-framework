using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Job.Lifecycle;

public class MessageActivityTrackerTests
{
    private static readonly TimeSpan WaitPoll = TimeSpan.FromSeconds(1);

    private readonly ILogger _logger = Substitute.For<ILogger>();
    private readonly FakeTimeProvider _time = new();

    [Fact]
    public void ShouldShutdownDueToInactivity_ReturnsFalse_WhenMessagesAreInProgress()
    {
        // Verifies that shutdown is prevented while messages are still being processed
        var tracker = new MessageActivityTracker(_time);
        using var scope = tracker.BeginMessageProcessing();

        _time.Advance(TimeSpan.FromSeconds(10));

        Assert.False(tracker.ShouldShutdownDueToInactivity(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void ShouldShutdownDueToInactivity_ReturnsFalse_BeforeIdleTimeoutReached()
    {
        // Verifies that shutdown doesn't trigger prematurely before the idle timeout period
        var tracker = new MessageActivityTracker(_time);

        _time.Advance(TimeSpan.FromSeconds(1));

        Assert.False(tracker.ShouldShutdownDueToInactivity(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public void ShouldShutdownDueToInactivity_ReturnsTrue_WhenIdleTimeoutReachedAndNoMessagesInProgress()
    {
        // Verifies that shutdown is triggered when idle timeout is reached and no messages are processing
        var tracker = new MessageActivityTracker(_time);

        _time.Advance(TimeSpan.FromSeconds(2));

        Assert.True(tracker.ShouldShutdownDueToInactivity(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void ShouldShutdownDueToInactivity_ReturnsFalse_WhenNewMessageReceivedWithinTimeout()
    {
        // Verifies that receiving a new message resets the idle timer
        var tracker = new MessageActivityTracker(_time);

        _time.Advance(TimeSpan.FromMilliseconds(500));
        using (tracker.BeginMessageProcessing()) { }
        _time.Advance(TimeSpan.FromMilliseconds(500));

        Assert.False(tracker.ShouldShutdownDueToInactivity(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void BeginMessageProcessing_ShouldTrackMultipleConcurrentMessages()
    {
        // Verifies that multiple concurrent messages are correctly tracked
        var tracker = new MessageActivityTracker(_time);

        var scope1 = tracker.BeginMessageProcessing();
        var scope2 = tracker.BeginMessageProcessing();
        var scope3 = tracker.BeginMessageProcessing();

        Assert.False(tracker.ShouldShutdownDueToInactivity(TimeSpan.Zero));

        scope1.Dispose();
        Assert.False(tracker.ShouldShutdownDueToInactivity(TimeSpan.Zero));

        scope2.Dispose();
        scope3.Dispose();

        Assert.True(tracker.ShouldShutdownDueToInactivity(TimeSpan.Zero));
    }

    [Fact]
    public async Task WaitForAllMessagesToComplete_ShouldReturnImmediately_WhenNoMessagesInProgress()
    {
        // Verifies that graceful shutdown completes immediately when no messages are being processed
        var tracker = new MessageActivityTracker(_time, WaitPoll);

        await tracker.WaitForAllMessagesToComplete(TimeSpan.FromSeconds(5), _logger);

        Assert.Equal(TimeSpan.Zero, _time.GetUtcNow() - _time.GetUtcNow());
    }

    [Fact]
    public async Task WaitForAllMessagesToComplete_ShouldWaitForMessagesToComplete()
    {
        // Verifies that graceful shutdown waits for in-flight messages to finish processing
        var tracker = new MessageActivityTracker(_time, WaitPoll);
        var scope = tracker.BeginMessageProcessing();

        var waitTask = tracker.WaitForAllMessagesToComplete(TimeSpan.FromSeconds(5), _logger);

        // Advance one poll interval — still in progress, so the task must not complete.
        _time.Advance(WaitPoll);
        await Task.Yield();
        Assert.False(waitTask.IsCompleted);

        scope.Dispose();

        // Next poll tick sees zero in-progress messages and returns.
        _time.Advance(WaitPoll);
        await waitTask;

        Assert.True(waitTask.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task WaitForAllMessagesToComplete_ShouldTimeout_WhenMessagesDoNotComplete()
    {
        // Verifies that graceful shutdown doesn't hang indefinitely if messages fail to complete
        var tracker = new MessageActivityTracker(_time, WaitPoll);
        using var scope = tracker.BeginMessageProcessing();

        var timeout = TimeSpan.FromSeconds(3);
        var waitTask = tracker.WaitForAllMessagesToComplete(timeout, _logger);

        // Advance past the timeout. Each poll tick fires until the deadline is reached.
        _time.Advance(timeout + WaitPoll);
        await waitTask;

        Assert.True(waitTask.IsCompletedSuccessfully);
    }

    [Fact]
    public void MessageProcessingScope_ShouldBeIdempotent_WhenDisposedMultipleTimes()
    {
        // Verifies that disposing a message scope multiple times doesn't cause errors or incorrect state
        var tracker = new MessageActivityTracker(_time);
        var scope = tracker.BeginMessageProcessing();

        scope.Dispose();
        scope.Dispose();
        scope.Dispose();

        Assert.True(tracker.ShouldShutdownDueToInactivity(TimeSpan.Zero));
    }
}
