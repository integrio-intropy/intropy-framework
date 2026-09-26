using System.Collections.Concurrent;
using System.Diagnostics;
using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.RunToCompletion;
using Intropy.Framework.Hosting.TransactionalIntegration.Helpers;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Lifecycle;

/// <summary>
/// Manages the message subscription lifecycle, including receiving messages,
/// monitoring for idle timeout, and coordinating graceful shutdown.
/// </summary>
/// <param name="topicSubscriber">The topic subscriber for subscribing to messages.</param>
/// <param name="options">Configuration options for the integration.</param>
/// <param name="sendPipeline">The pipeline to execute when a message is read from the queue.</param>
/// <param name="componentName">The component name, for logs and errors.</param>
/// <param name="contextFactory">Creates the context for each message, from the metadata
/// propagated with it.</param>
/// <param name="logger">An instance of <see cref="ILogger"/> for logging.</param>
/// <typeparam name="TCtx">The send pipeline's context type.</typeparam>
internal class MessageSubscriber<TCtx>(
    ITopicSubscriber topicSubscriber,
    ISendPipeline<TCtx> sendPipeline,
    TransactionalIntegrationOptions options,
    string componentName,
    ContextFactory<TCtx> contextFactory,
    ILogger<MessageSubscriber<TCtx>> logger) where TCtx : Context
{
    private readonly MessageActivityTracker _activityTracker = new();

    /// <summary>Each message's latest outcome in this run, by message id: a redelivered message
    /// that succeeds later in the run counts once, as processed.</summary>
    private readonly ConcurrentDictionary<string, MessageOutcome> _outcomes = new();

    /// <summary>The span of the run consuming the messages (the job's), linked from each
    /// message's span.</summary>
    private ActivityContext _run;

    /// <summary>The host's cancellation. A message interrupted because the host is stopping is
    /// retried but not counted, the way the sweep treats an interrupted file.</summary>
    private CancellationToken _hostCancellation;

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
    public async Task<JobRunSummary> ExecuteAsync(Task publishingCompleteSignal, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publishingCompleteSignal);
        using var shutdownSignal = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _run = Activity.Current?.Context ?? default;
        _hostCancellation = ct;
        var unfinished = 0;

        var subscription = await topicSubscriber.SubscribeAsync(
            options.DaprPubSubName,
            options.DaprTopicName,
            options.MaxMessageProcessingTime,
            HandleMessageAsync,
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

    private JobRunSummary Summarize(int unfinished)
    {
        var outcomes = _outcomes.Values.ToList();
        var summary = new JobRunSummary(
            Processed: outcomes.Count(o => o == MessageOutcome.Processed),
            Failed: outcomes.Count(o => o == MessageOutcome.Failed) + unfinished,
            Skipped: outcomes.Count(o => o == MessageOutcome.Skipped));
        logger.LogInformation(
            "Consumed messages for {Component}: {Processed} processed, {Failed} left for redelivery, {Skipped} skipped",
            componentName, summary.Processed, summary.Failed, summary.Skipped);
        return summary;
    }

    private async Task<TopicResponseAction> HandleMessageAsync(TopicMessage message,
        CancellationToken cancellationToken)
    {
        using var activity = DaprActivityHelper.StartProcessActivity(message, options.DaprTopicName, componentName, _run);
        using var messageScope = _activityTracker.BeginMessageProcessing();

        try
        {
            var (metadata, isRetry) = ContextHelper.Restore(message);
            var initialContext = ContextCreation.Create(contextFactory, metadata, isRetry, componentName, message.Id);
            var (result, _) = await sendPipeline.Execute(message.Data, initialContext, cancellationToken);

            // A retried message was not processed: the consumer span is an error either way.
            switch (result)
            {
                case StepResult<string>.TechnicalFailure failure:
                    Fail(activity, "technical_failure", failure.Value.Description);
                    return Record(message, MessageOutcome.Failed, TopicResponseAction.Retry);
                case StepResult<string>.BusinessFailure failure:
                    Fail(activity, "business_failure", failure.Value.Description);
                    return Record(message, MessageOutcome.Failed, TopicResponseAction.Retry);
                case StepResult<string>.Cancelled:
                    return Record(message, MessageOutcome.Skipped, TopicResponseAction.Success);
                case StepResult<string>.Aborted:
                    // Never acknowledge an interrupted message: it was not processed.
                    return Interrupted(message, activity);
                default:
                    return Record(message, MessageOutcome.Processed, TopicResponseAction.Success);
            }
        }
        catch (OperationCanceledException) when (_hostCancellation.IsCancellationRequested)
        {
            return Interrupted(message, activity);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing message");
            activity?.AddException(ex);
            Fail(activity, ex.GetType().FullName!, ex.Message);
            return Record(message, MessageOutcome.Failed, TopicResponseAction.Retry);
        }
    }

    /// <summary>Leaves an interrupted message for redelivery. Interrupted because the host is
    /// stopping, it is not counted and its span is not an error; interrupted otherwise (such as
    /// by the processing timeout), it is a failure.</summary>
    private TopicResponseAction Interrupted(TopicMessage message, Activity? activity)
    {
        if (_hostCancellation.IsCancellationRequested)
        {
            logger.LogInformation("Processing message {MessageId} was interrupted by the host stopping; it is left for redelivery",
                message.Id);
            return TopicResponseAction.Retry;
        }

        logger.LogWarning("Processing message {MessageId} was aborted without the host stopping; it is left for redelivery",
            message.Id);
        Fail(activity, "aborted", "Processing was aborted without the host stopping");
        return Record(message, MessageOutcome.Failed, TopicResponseAction.Retry);
    }

    private static void Fail(Activity? activity, string errorType, string description)
    {
        activity?.SetTag("error.type", errorType);
        activity?.SetStatus(ActivityStatusCode.Error, description);
    }

    private TopicResponseAction Record(TopicMessage message, MessageOutcome outcome, TopicResponseAction response)
    {
        _outcomes[message.Id] = outcome;
        return response;
    }

    private enum MessageOutcome
    {
        Processed,
        Skipped,
        Failed
    }
}
