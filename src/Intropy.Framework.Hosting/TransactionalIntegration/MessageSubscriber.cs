using System.Collections.Concurrent;
using System.Diagnostics;
using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// Manages the message subscription lifecycle: subscribes to the integration's topic, hands each
/// consumed message to the <see cref="MessageProcessor{TCtx}"/>, monitors for idle timeout, and
/// coordinates graceful shutdown. Tallies every message's latest outcome in the run for the
/// run's summary.
/// </summary>
/// <param name="topicSubscriber">The topic subscriber for subscribing to messages.</param>
/// <param name="processor">Processes each consumed message through the send pipeline.</param>
/// <param name="options">Configuration options for the integration.</param>
/// <param name="componentName">The component name, for logs and errors.</param>
/// <param name="logger">An instance of <see cref="ILogger"/> for logging.</param>
/// <param name="timeProvider">Time source for the idle clock. Defaults to
/// <see cref="TimeProvider.System"/>. Override in tests to control time.</param>
/// <typeparam name="TCtx">The send pipeline's context type.</typeparam>
internal class MessageSubscriber<TCtx>(
    ITopicSubscriber topicSubscriber,
    MessageProcessor<TCtx> processor,
    TransactionalIntegrationOptions options,
    string componentName,
    ILogger<MessageSubscriber<TCtx>> logger,
    TimeProvider? timeProvider = null) where TCtx : Context
{
    /// <summary>Subscription teardown happens while the sidecar is going away; the run must not
    /// hang forever on it. Bounded generously above any Dapr client's own timeouts.</summary>
    private static readonly TimeSpan s_disposeTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Starts the message subscription and waits for completion.
    /// This method will block until the idle timeout is reached and graceful shutdown completes.
    /// </summary>
    /// <param name="publishingCompleteSignal">A task that completes when all files have been published.</param>
    /// <param name="ct">The host's cancellation: it stops waiting for the publisher and the idle
    /// timeout, and in-flight messages still get the grace period before the subscription closes.</param>
    /// <returns>The messages this run consumed, by their last outcome: acknowledged as
    /// <c>Processed</c>, idempotent duplicates as <c>Skipped</c>, and messages left for
    /// redelivery — returned for retry, or still in flight when the grace period ended — as
    /// <c>Failed</c>. A message interrupted because the host is stopping is left for redelivery
    /// but not counted: host cancellation is not a failure.</returns>
    public async Task<RunSummary> ExecuteAsync(Task publishingCompleteSignal, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publishingCompleteSignal);
        using var shutdownSignal = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var run = Activity.Current?.Context ?? default;
        var unfinished = 0;

        // The run's state is the run's, not the singleton subscriber's: the outcome tally and
        // idle clock must live and die with ExecuteAsync, so two runs can never see each other.
        var activityTracker = new MessageActivityTracker(timeProvider);
        var outcomes = new ConcurrentDictionary<string, MessageOutcome>();

        var subscription = await topicSubscriber.SubscribeAsync(
            options.DaprPubSubName,
            options.DaprTopicName,
            options.MaxMessageProcessingTime,
            (message, messageCt) => HandleMessageAsync(message, run, activityTracker, outcomes, ct, messageCt),
            CancellationToken.None);

        try
        {
            // Wait for publishing to complete before starting idle monitor
            logger.LogInformation("Waiting for publisher to complete...");
            await publishingCompleteSignal.WaitAsync(shutdownSignal.Token);
            logger.LogInformation("Publisher completed. Starting idle timeout monitoring.");

            // The idle window measures quiet time after the sweep, not the process's whole
            // lifetime: the clock restarts here so a sweep longer than IdleTimeout cannot make
            // the first poll look already-idle and exit with messages still on the queue.
            activityTracker.RestartIdleClock();

            // Start the idle monitor only after publisher completes
            var idleMonitor = new IdleTimeoutMonitor(
                activityTracker,
                options.IdleTimeout,
                logger,
                timeProvider);

            await idleMonitor.WaitForIdleTimeoutAsync(shutdownSignal.Token);

            logger.LogInformation("Idle timeout triggered. Beginning graceful shutdown.");
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Subscription was cancelled.");
        }
        finally
        {
            // Wait for in-flight messages while the subscription is still open,
            // so Dapr can still deliver ack/nack responses for those messages.
            unfinished = await activityTracker.WaitForAllMessagesToComplete(
                options.PostIdleGracePeriod,
                logger);

            await shutdownSignal.CancelAsync();

            await DisposeSubscriptionAsync(subscription);
        }

        return Summarize(ct.IsCancellationRequested ? 0 : unfinished, outcomes);
    }

    private async Task DisposeSubscriptionAsync(IAsyncDisposable subscription)
    {
        // JobRunner shuts the sidecar down as soon as the run returns; if the subscription's own
        // teardown wedges, an otherwise-finished run must not hang with it.
        try
        {
            await subscription.DisposeAsync().AsTask().WaitAsync(s_disposeTimeout, CancellationToken.None);
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "Subscription teardown for {Component} did not complete cleanly", componentName);
        }
    }

    private async Task<TopicResponseAction> HandleMessageAsync(TopicMessage message, ActivityContext run,
        MessageActivityTracker activityTracker, ConcurrentDictionary<string, MessageOutcome> outcomes,
        CancellationToken hostCancellation, CancellationToken cancellationToken)
    {
        using var messageScope = activityTracker.BeginMessageProcessing();
        var result = await processor.ProcessAsync(message, run, hostCancellation, cancellationToken);

        // The latest outcome in the run counts; an interruption by the host stopping does not
        // replace an earlier one.
        if (result.Outcome is not MessageOutcome.Interrupted)
            outcomes[message.Id] = result.Outcome;
        return result.Response;
    }

    private RunSummary Summarize(int unfinished, ConcurrentDictionary<string, MessageOutcome> outcomes)
    {
        var tallied = outcomes.Values.ToList();
        var summary = new RunSummary(
            Processed: tallied.Count(o => o == MessageOutcome.Processed),
            Failed: tallied.Count(o => o == MessageOutcome.Failed) + unfinished,
            Skipped: tallied.Count(o => o == MessageOutcome.Skipped));
        logger.LogInformation(
            "Consumed messages for {Component}: {Processed} processed, {Failed} left for redelivery, {Skipped} skipped",
            componentName, summary.Processed, summary.Failed, summary.Skipped);
        return summary;
    }
}
