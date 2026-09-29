using System.Diagnostics;
using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.Logging;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// Processes one consumed message through the send pipeline: restores the context propagated
/// with the message, runs the pipeline inside the message's own consumer span, and maps the
/// result to the Dapr response and the message's <see cref="MessageOutcome"/> in the run. Owns
/// nothing about the subscription itself — that is <see cref="MessageSubscriber{TCtx}"/>.
/// </summary>
/// <param name="sendPipeline">The pipeline to execute when a message is read from the queue.</param>
/// <param name="contextFactory">Creates the context for each message, from the metadata
/// propagated with it.</param>
/// <param name="componentName">The component name, for logs and metrics.</param>
/// <param name="topicName">The topic the messages are consumed from, for spans and metrics.</param>
/// <param name="logger">An instance of <see cref="ILogger"/> for logging.</param>
/// <typeparam name="TCtx">The send pipeline's context type.</typeparam>
internal sealed class MessageProcessor<TCtx>(
    ISendPipeline<TCtx> sendPipeline,
    ContextFactory<TCtx> contextFactory,
    string componentName,
    string topicName,
    ILogger<MessageProcessor<TCtx>> logger) where TCtx : Context
{
    /// <summary>Runs <paramref name="message"/> through the send pipeline and records its metrics.</summary>
    /// <param name="message">The consumed message.</param>
    /// <param name="run">The span of the run consuming the messages (the job's), linked from the
    /// message's span.</param>
    /// <param name="hostCancellation">The host's cancellation: a message interrupted because the
    /// host is stopping is left for redelivery but not counted as failed.</param>
    /// <param name="cancellationToken">The message's processing cancellation.</param>
    /// <returns>The response to return to Dapr and the message's outcome in the run.</returns>
    internal async Task<MessageProcessingResult> ProcessAsync(TopicMessage message, ActivityContext run,
        CancellationToken hostCancellation, CancellationToken cancellationToken)
    {
        var start = Stopwatch.GetTimestamp();
        using var activity = DaprActivityHelper.StartProcessActivity(message, topicName, run);
        var (response, outcome, errorType) = await ExecutePipelineAsync(message, activity, hostCancellation,
            cancellationToken);
        HostingMetrics.RecordProcessedMessage(componentName, topicName, HostingMetrics.OutcomeName(outcome), errorType,
            Stopwatch.GetElapsedTime(start));
        return new MessageProcessingResult(response, outcome);
    }

    private async Task<(TopicResponseAction Response, MessageOutcome Outcome, string? ErrorType)> ExecutePipelineAsync(
        TopicMessage message, Activity? activity, CancellationToken hostCancellation, CancellationToken cancellationToken)
    {
        try
        {
            var (metadata, isRetry) = ContextHelper.Restore(message);
            var initialContext = ContextCreation.Create(contextFactory, metadata, isRetry, componentName, message.Id);
            var (result, _) = await sendPipeline.Execute(message.Data, initialContext, cancellationToken);

            // A retried message was not processed: the consumer span is an error either way.
            switch (result)
            {
                case StepResult<string>.TechnicalFailure failure:
                    return Failed(activity, "technical_failure", failure.Value.Description);
                case StepResult<string>.BusinessFailure failure:
                    return Failed(activity, "business_failure", failure.Value.Description);
                case StepResult<string>.Cancelled:
                    return (TopicResponseAction.Success, MessageOutcome.Skipped, null);
                case StepResult<string>.Aborted:
                    // Never acknowledge an interrupted message: it was not processed.
                    return Interrupted(message, activity, hostCancellation);
                default:
                    return (TopicResponseAction.Success, MessageOutcome.Processed, null);
            }
        }
        catch (OperationCanceledException) when (hostCancellation.IsCancellationRequested)
        {
            return Interrupted(message, activity, hostCancellation);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing message");
            activity?.AddException(ex);
            return Failed(activity, ex.GetType().FullName!, ex.Message);
        }
    }

    /// <summary>Leaves an interrupted message for redelivery. Interrupted because the host is
    /// stopping, it is not counted and its span is not an error; interrupted otherwise (such as
    /// by the processing timeout), it is a failure.</summary>
    private (TopicResponseAction, MessageOutcome, string?) Interrupted(TopicMessage message, Activity? activity,
        CancellationToken hostCancellation)
    {
        if (hostCancellation.IsCancellationRequested)
        {
            logger.LogInformation("Processing message {MessageId} was interrupted by the host stopping; it is left for redelivery",
                message.Id);
            return (TopicResponseAction.Retry, MessageOutcome.Interrupted, null);
        }

        logger.LogWarning("Processing message {MessageId} was aborted without the host stopping; it is left for redelivery",
            message.Id);
        return Failed(activity, "aborted", "Processing was aborted without the host stopping");
    }

    private static (TopicResponseAction, MessageOutcome, string?) Failed(Activity? activity, string errorType,
        string description)
    {
        activity?.SetTag("error.type", errorType);
        activity?.SetStatus(ActivityStatusCode.Error, description);
        return (TopicResponseAction.Retry, MessageOutcome.Failed, errorType);
    }
}

/// <summary>How one handled message ended: the response to return to Dapr and its outcome in the run.</summary>
internal readonly record struct MessageProcessingResult(TopicResponseAction Response, MessageOutcome Outcome);
