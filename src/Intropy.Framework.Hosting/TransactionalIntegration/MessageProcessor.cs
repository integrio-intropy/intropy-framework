using System.Diagnostics;
using System.Text.Json;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// The Transactional Integration's <see cref="MessageHandler"/>: restores the context propagated
/// with the message, runs the message's data through the send pipeline, and maps the result to the
/// message's outcome. Spans, metrics, the time limit and the ack are the
/// <see cref="MessageConsumer"/>'s.
/// </summary>
/// <param name="sendPipeline">The pipeline to execute when a message is read from the queue.</param>
/// <param name="contextFactory">Creates the context for each message, from the metadata
/// propagated with it.</param>
/// <param name="componentName">The component name, for logs and errors.</param>
/// <param name="logger">An instance of <see cref="ILogger"/> for logging.</param>
/// <typeparam name="TCtx">The send pipeline's context type.</typeparam>
internal sealed class MessageProcessor<TCtx>(
    ISendPipeline<TCtx> sendPipeline,
    ContextFactory<TCtx> contextFactory,
    string componentName,
    ILogger<MessageProcessor<TCtx>> logger) where TCtx : Context
{
    /// <summary>The CloudEvent extension the receive side propagates the context metadata in.</summary>
    private const string MetadataKey = "metadata";

    internal async Task<HandledMessage> HandleAsync(IncomingMessage message, Activity? activity,
        CancellationToken interrupt, CancellationToken cancellationToken)
    {
        var metadata = RestoreMetadata(message);
        var context = ContextCreation.Create(contextFactory, metadata, message.IsRetry, componentName, message.MessageId);
        var (result, _) = await sendPipeline.Execute(message.Data, context, cancellationToken);

        switch (result)
        {
            // A Transactional Integration leaves a business failure for redelivery too: the message
            // was not delivered, and the broker dead-letters it once redelivery gives up.
            case StepResult<string>.TechnicalFailure failure:
                return Failed("technical_failure", failure.Value.Description);
            case StepResult<string>.BusinessFailure failure:
                return Failed("business_failure", failure.Value.Description);
            case StepResult<string>.Cancelled:
                return new HandledMessage(new PipelineOutcome(MessageOutcome.Skipped));
            case StepResult<string>.Aborted:
                return Aborted(message, interrupt);
            default:
                return new HandledMessage(new PipelineOutcome(MessageOutcome.Processed));
        }
    }

    /// <summary>The context metadata propagated with <paramref name="message"/>, plus its message id.</summary>
    internal static Dictionary<string, string> RestoreMetadata(IncomingMessage message)
    {
        var metadata = message.GetExtension(MetadataKey) is { Length: > 0 } json
            ? JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? []
            : [];
        metadata.TryAdd(ContextKeys.MessageId, message.MessageId);
        return metadata;
    }

    /// <summary>An aborted pipeline: interrupted when the consumer interrupted it (not counted as
    /// failed), a failure otherwise (such as the processing time limit).</summary>
    private HandledMessage Aborted(IncomingMessage message, CancellationToken interrupt)
    {
        if (interrupt.IsCancellationRequested)
        {
            logger.LogInformation("Processing message {MessageId} was interrupted by the host stopping; it is left for redelivery",
                message.MessageId);
            return new HandledMessage(new PipelineOutcome(MessageOutcome.Interrupted));
        }

        logger.LogWarning("Processing message {MessageId} was aborted without the host stopping; it is left for redelivery",
            message.MessageId);
        return Failed("aborted", "Processing was aborted without the host stopping");
    }

    private static HandledMessage Failed(string errorType, string description) =>
        new(PipelineOutcome.Failed(errorType, description));
}
