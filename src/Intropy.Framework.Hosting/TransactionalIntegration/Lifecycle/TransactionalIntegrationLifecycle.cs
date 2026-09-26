using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.RunToCompletion;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Lifecycle;

/// <summary>
/// Represents the entire lifecycle of the Transactional Integration: sweeps the source, publishing
/// each file to the integration's queue through the receive pipeline, while the subscriber
/// consumes the queue through the send pipeline. Hosted by the <see cref="RunToCompletionRunner"/>,
/// which owns the sidecar lifecycle, job tracing, and exit-code mapping.
/// </summary>
/// <typeparam name="TCtx">The integration's context type, shared by the receive and send pipelines.</typeparam>
public class TransactionalIntegrationLifecycle<TCtx> : IRunToCompletionJob where TCtx : Context
{
    private readonly TransactionalIntegrationReceiveJob<TCtx> _receiveJob;
    private readonly MessageSubscriber<TCtx> _messageSubscriber;
    private readonly ILogger<TransactionalIntegrationLifecycle<TCtx>> _logger;

    /// <summary>
    /// Creates a new instance of <see cref="TransactionalIntegrationLifecycle{TCtx}"/>.
    /// </summary>
    /// <param name="receiveJob">The receive side: sweeps the integration's source through the
    /// receive pipeline, completing a file (delete or archive) only after it is on the queue.</param>
    /// <param name="topicSubscriber">An instance of <see cref="ITopicSubscriber"/> used for receiving messages from the queue.</param>
    /// <param name="sendPipeline">An instance of <see cref="ISendPipeline{TCtx}"/> that is invoked once a message is received.</param>
    /// <param name="options">An instance of <see cref="TransactionalIntegrationOptions"/>.</param>
    /// <param name="componentName">The component name, for logs and errors.</param>
    /// <param name="contextFactory">Creates the send pipeline's context for each message.</param>
    /// <param name="loggerFactory">An instance of <see cref="ILoggerFactory"/>.</param>
    internal TransactionalIntegrationLifecycle(
        TransactionalIntegrationReceiveJob<TCtx> receiveJob,
        ITopicSubscriber topicSubscriber,
        ISendPipeline<TCtx> sendPipeline,
        TransactionalIntegrationOptions options,
        string componentName,
        ContextFactory<TCtx> contextFactory,
        ILoggerFactory loggerFactory)
    {
        _receiveJob = receiveJob;
        _logger = loggerFactory.CreateLogger<TransactionalIntegrationLifecycle<TCtx>>();

        var messageSubscriberLogger = loggerFactory.CreateLogger<MessageSubscriber<TCtx>>();
        _messageSubscriber = new MessageSubscriber<TCtx>(topicSubscriber, sendPipeline, options, componentName,
            contextFactory, messageSubscriberLogger);
    }

    /// <summary>
    /// Runs the transactional integration lifecycle: sweeps the source while subscribing, and
    /// returns once publishing is done and the subscription has gone idle (or the host cancelled).
    /// </summary>
    /// <returns>The source sweep's outcome: files published (and completed), and files left in
    /// place as failed.</returns>
    public async Task<JobRunSummary> ExecuteAsync(CancellationToken ct)
    {
        var coordinator = new LifecycleCoordinator();

        var publisherTask = Task.Run(async () =>
        {
            try
            {
                _logger.LogInformation("Starting source item processing");
                var summary = await _receiveJob.ExecuteAsync(ct);
                _logger.LogInformation("Source item processing completed");
                coordinator.SignalPublishingComplete();
                return summary;
            }
            catch (Exception ex)
            {
                coordinator.SignalPublishingFailed(ex);
                throw;
            }
        });

        var subscriberTask = _messageSubscriber.ExecuteAsync(coordinator.PublishingCompleteSignal, ct);

        await Task.WhenAll(publisherTask, subscriberTask);
        return await publisherTask;
    }
}
