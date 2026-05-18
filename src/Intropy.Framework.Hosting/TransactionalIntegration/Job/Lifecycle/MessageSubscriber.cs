using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.TransactionalIntegration.Job.Helpers;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;

/// <summary>
/// Manages the message subscription lifecycle, including receiving messages,
/// monitoring for idle timeout, and coordinating graceful shutdown.
/// </summary>
/// <param name="topicSubscriber">The topic subscriber for subscribing to messages.</param>
/// <param name="options">Configuration options for the integration.</param>
/// <param name="sendPipeline">The pipeline to execute when a message is read from the queue.</param>
/// <param name="logger">An instance of <see cref="ILogger"/> for logging.</param>
public class MessageSubscriber(
    ITopicSubscriber topicSubscriber,
    ISendPipeline<Context> sendPipeline,
    TransactionalIntegrationOptions options,
    ILogger<MessageSubscriber> logger)
{
    private readonly MessageActivityTracker _activityTracker = new();

    /// <summary>
    /// Starts the message subscription and waits for completion.
    /// This method will block until the idle timeout is reached and graceful shutdown completes.
    /// </summary>
    /// <param name="publishingCompleteSignal">A task that completes when all files have been published.</param>
    public async Task ExecuteAsync(Task publishingCompleteSignal)
    {
        using var shutdownSignal = new CancellationTokenSource();

        var subscription = await topicSubscriber.SubscribeAsync(
            options.DaprPubSubName,
            options.DaprTopicName,
            TimeSpan.FromSeconds(options.MaxMessageProcessingTimeSeconds),
            HandleMessageAsync,
            CancellationToken.None);

        try
        {
            // Wait for publishing to complete before starting idle monitor
            logger.LogInformation("Waiting for publisher to complete...");
            await publishingCompleteSignal;
            logger.LogInformation("Publisher completed. Starting idle timeout monitoring.");

            // Start the idle monitor only after publisher completes
            var idleMonitor = new IdleTimeoutMonitor(
                _activityTracker,
                TimeSpan.FromSeconds(options.IdleTimeoutSeconds),
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
            await _activityTracker.WaitForAllMessagesToComplete(
                TimeSpan.FromSeconds(options.PostIdleGracePeriodSeconds),
                logger);

            await shutdownSignal.CancelAsync();

            // Dispose the subscription after all in-flight messages have completed.
            await subscription.DisposeAsync();
        }
    }

    private async Task<TopicResponseAction> HandleMessageAsync(TopicMessage message,
        CancellationToken cancellationToken)
    {
        using var activity = DaprActivityHelper.Restore(message.Extensions);
        using var messageScope = _activityTracker.BeginMessageProcessing();

        try
        {
            var initialContext = ContextHelper.Restore(message);
            var (result, _) = await sendPipeline.Execute(message.Data, initialContext, cancellationToken);

            return result switch
            {
                StepResult<string>.TechnicalFailure => TopicResponseAction.Retry,
                StepResult<string>.BusinessFailure => TopicResponseAction.Retry,
                _ => TopicResponseAction.Success
            };
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing message");
            return TopicResponseAction.Retry;
        }
    }
}
