using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;

/// <summary>
/// Represents the entire lifecycle of the Transactional Integration.
/// Coordinates listing items, processing them through the receive pipeline,
/// and consuming messages via the subscriber.
/// </summary>
public class TransactionalIntegrationLifecycle
{
    private readonly ISourceLister _sourceLister;
    private readonly IReceivePipeline<Context> _receivePipeline;
    private readonly MessageSubscriber _messageSubscriber;
    private readonly ILogger<TransactionalIntegrationLifecycle> _logger;

    /// <summary>
    /// Creates a new instance of <see cref="TransactionalIntegrationLifecycle"/>.
    /// </summary>
    /// <param name="sourceLister">An instance of <see cref="ISourceLister"/> for listing items to process.</param>
    /// <param name="receivePipeline">An instance of <see cref="IReceivePipeline{TCtx}"/> for processing items.</param>
    /// <param name="topicSubscriber">An instance of <see cref="ITopicSubscriber"/> used for receiving messages from the queue.</param>
    /// <param name="sendPipeline">An instance of <see cref="ISendPipeline{TCtx}"/> that is invoked once a message is received.</param>
    /// <param name="options">An instance of <see cref="TransactionalIntegrationOptions"/>.</param>
    /// <param name="loggerFactory">An instance of <see cref="ILoggerFactory"/>.</param>
    public TransactionalIntegrationLifecycle(
        ISourceLister sourceLister,
        IReceivePipeline<Context> receivePipeline,
        ITopicSubscriber topicSubscriber,
        ISendPipeline<Context> sendPipeline,
        TransactionalIntegrationOptions options,
        ILoggerFactory loggerFactory)
    {
        _sourceLister = sourceLister;
        _receivePipeline = receivePipeline;
        _logger = loggerFactory.CreateLogger<TransactionalIntegrationLifecycle>();

        var messageSubscriberLogger = loggerFactory.CreateLogger<MessageSubscriber>();
        _messageSubscriber = new MessageSubscriber(topicSubscriber, sendPipeline, options, messageSubscriberLogger);
    }

    /// <summary>
    /// Starts the execution of the transactional integration lifecycle.
    /// </summary>
    public virtual async Task Start()
    {
        var coordinator = new LifecycleCoordinator();

        var publisherTask = Task.Run(async () =>
        {
            try
            {
                await ProcessSourceItemsAsync();
                coordinator.SignalPublishingComplete();
            }
            catch (Exception ex)
            {
                coordinator.SignalPublishingFailed(ex);
                throw;
            }
        });

        var subscriberTask = _messageSubscriber.ExecuteAsync(coordinator.PublishingCompleteSignal);

        await Task.WhenAll(publisherTask, subscriberTask);
    }

    private async Task ProcessSourceItemsAsync()
    {
        _logger.LogInformation("Starting source item processing");

        var listResult = await _sourceLister.ListItemsAsync();
        await ProcessItemsAsync(listResult);

        _logger.LogInformation("Source item processing completed");
    }

    private async Task ProcessItemsAsync(IReadOnlyList<SourceItemInfo> items)
    {
        foreach (var item in items)
        {
            var context = new Context(new Dictionary<string, string> { { "sourceItemId", item.Id } });
            var (result, _) = await _receivePipeline.Execute(item, context);

            switch (result)
            {
                case StepResult<SourceItem>.TechnicalFailure tf:
                    _logger.LogError(tf.Value.Exception,
                        "Technical failure processing source item: {ItemId} - {Message}",
                        item.Id, tf.Value.Description);
                    break;
                case StepResult<SourceItem>.BusinessFailure:
                    _logger.LogError("Business failure processing source item: {ItemId}", item.Id);
                    break;
            }
        }
    }
}
