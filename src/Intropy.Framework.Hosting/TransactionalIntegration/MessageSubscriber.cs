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
/// <typeparam name="TCtx">The send pipeline's context type.</typeparam>
internal class MessageSubscriber<TCtx>(
    ITopicSubscriber topicSubscriber,
    MessageProcessor<TCtx> processor,
    TransactionalIntegrationOptions options,
    string componentName,
    ILogger<MessageSubscriber<TCtx>> logger) where TCtx : Context
{
    private readonly MessageActivityTracker _activityTracker = new();

    /// <summary>Each message's latest outcome in this run, by message id: a redelivered message
    /// that succeeds later in the run counts once, as processed.</summary>
    private readonly ConcurrentDictionary<string, MessageOutcome> _outcomes = new();

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

        var subscription = await topicSubscriber.SubscribeAsync(
            options.DaprPubSubName,
            options.DaprTopicName,
            options.MaxMessageProcessingTime,
            (message, messageCt) => HandleMessageAsync(message, run, ct, messageCt),
            CancellationToken.None);

        try
        {
            // Wait for publishing to complete before starting idle monitor
            logger.LogInformation("Waiting for publisher to complete...");
            await publishingCompleteSignal.WaitAsync(shutdownSignal.Token);
            logger.LogInformation("Publisher completed. Starting idle timeout monitoring.");

            // Start the idle monitor only after publisher completes
            var idleMonitor = new IdleTimeoutMonitor(
                _activityTracker,
                options.IdleTimeout,
                logger);

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
            unfinished = await _activityTracker.WaitForAllMessagesToComplete(
                options.PostIdleGracePeriod,
                logger);

            await shutdownSignal.CancelAsync();

            // Dispose the subscription after all in-flight messages have completed.
            await subscription.DisposeAsync();
        }

        return Summarize(ct.IsCancellationRequested ? 0 : unfinished);
    }

    private async Task<TopicResponseAction> HandleMessageAsync(TopicMessage message, ActivityContext run,
        CancellationToken hostCancellation, CancellationToken cancellationToken)
    {
        using var messageScope = _activityTracker.BeginMessageProcessing();
        var result = await processor.ProcessAsync(message, run, hostCancellation, cancellationToken);

        // The latest outcome in the run counts; an interruption by the host stopping does not
        // replace an earlier one.
        if (result.Outcome is not MessageOutcome.Interrupted)
            _outcomes[message.Id] = result.Outcome;
        return result.Response;
    }

    private RunSummary Summarize(int unfinished)
    {
        var outcomes = _outcomes.Values.ToList();
        var summary = new RunSummary(
            Processed: outcomes.Count(o => o == MessageOutcome.Processed),
            Failed: outcomes.Count(o => o == MessageOutcome.Failed) + unfinished,
            Skipped: outcomes.Count(o => o == MessageOutcome.Skipped));
        logger.LogInformation(
            "Consumed messages for {Component}: {Processed} processed, {Failed} left for redelivery, {Skipped} skipped",
            componentName, summary.Processed, summary.Failed, summary.Skipped);
        return summary;
    }
}
