using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.Common;
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

    /// <summary>
    /// Starts the message subscription and waits for completion.
    /// This method will block until the idle timeout is reached and graceful shutdown completes.
    /// </summary>
    /// <param name="publishingCompleteSignal">A task that completes when all files have been published.</param>
    /// <param name="ct">The host's cancellation: it stops waiting for the publisher and the idle
    /// timeout, and in-flight messages still get the grace period before the subscription closes.</param>
    public async Task ExecuteAsync(Task publishingCompleteSignal, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publishingCompleteSignal);
        using var shutdownSignal = CancellationTokenSource.CreateLinkedTokenSource(ct);

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
            await _activityTracker.WaitForAllMessagesToComplete(
                options.PostIdleGracePeriod,
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
            var (metadata, isRetry) = ContextHelper.Restore(message);
            var initialContext = ContextCreation.Create(contextFactory, metadata, isRetry, componentName, message.Id);
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
