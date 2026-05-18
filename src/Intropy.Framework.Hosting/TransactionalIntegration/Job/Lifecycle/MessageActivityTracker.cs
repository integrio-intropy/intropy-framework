using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;

/// <summary>
/// Tracks message processing activity in a thread-safe manner.
/// </summary>
public class MessageActivityTracker
{
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _waitPollInterval;
    private readonly Lock _lock = new();
    private DateTimeOffset _lastMessageReceivedAt;
    private int _messagesInProgress;

    /// <summary>
    /// Creates a new <see cref="MessageActivityTracker"/>.
    /// </summary>
    /// <param name="timeProvider">Time source. Defaults to <see cref="TimeProvider.System"/>. Override in tests to control time.</param>
    /// <param name="waitPollInterval">Polling interval used by <see cref="WaitForAllMessagesToComplete"/>. Defaults to 1 second.</param>
    public MessageActivityTracker(TimeProvider? timeProvider = null, TimeSpan? waitPollInterval = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _waitPollInterval = waitPollInterval ?? TimeSpan.FromSeconds(1);
        _lastMessageReceivedAt = _timeProvider.GetUtcNow();
    }

    /// <summary>
    /// Begins tracking a new message being processed.
    /// Returns a disposable that will automatically mark the message as complete.
    /// </summary>
    public IDisposable BeginMessageProcessing()
    {
        lock (_lock)
        {
            _lastMessageReceivedAt = _timeProvider.GetUtcNow();
            _messagesInProgress++;
        }

        return new MessageProcessingScope(this);
    }

    /// <summary>
    /// Checks if the system should shut down due to inactivity.
    /// Returns true if the idle timeout has been reached AND no messages are being processed.
    /// </summary>
    public bool ShouldShutdownDueToInactivity(TimeSpan idleTimeout)
    {
        lock (_lock)
        {
            var timeSinceLastMessage = _timeProvider.GetUtcNow() - _lastMessageReceivedAt;
            var isIdle = timeSinceLastMessage >= idleTimeout;
            var noMessagesInProgress = _messagesInProgress == 0;

            return isIdle && noMessagesInProgress;
        }
    }

    /// <summary>
    /// Waits for all in-progress messages to complete, up to the specified timeout.
    /// </summary>
    public async Task WaitForAllMessagesToComplete(TimeSpan timeout, ILogger logger)
    {
        var deadline = _timeProvider.GetUtcNow() + timeout;

        while (_timeProvider.GetUtcNow() < deadline)
        {
            var currentCount = GetMessagesInProgress();

            if (currentCount == 0)
            {
                logger.LogInformation("All messages completed successfully.");
                return;
            }

            logger.LogInformation("Waiting for {MessageCount} message(s) to complete...", currentCount);

            await Task.Delay(_waitPollInterval, _timeProvider);
        }

        var remainingCount = GetMessagesInProgress();
        if (remainingCount > 0)
        {
            logger.LogWarning("Graceful shutdown timeout reached. {MessageCount} message(s) still in progress.",
                remainingCount);
        }
    }

    private int GetMessagesInProgress()
    {
        lock (_lock)
        {
            return _messagesInProgress;
        }
    }

    private void EndMessageProcessing()
    {
        lock (_lock)
        {
            _messagesInProgress--;
        }
    }

    private class MessageProcessingScope(MessageActivityTracker tracker) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;

            tracker.EndMessageProcessing();
            _disposed = true;
        }
    }
}
