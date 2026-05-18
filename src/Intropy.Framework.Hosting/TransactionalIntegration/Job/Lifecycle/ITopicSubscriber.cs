using Dapr.Messaging.PublishSubscribe;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;

/// <summary>
/// Abstraction for subscribing to messages from a pub/sub topic.
/// </summary>
public interface ITopicSubscriber
{
    /// <summary>
    /// Subscribes to messages from a topic.
    /// </summary>
    /// <param name="pubSubName">The name of the pub/sub component.</param>
    /// <param name="topicName">The name of the topic to subscribe to.</param>
    /// <param name="maxMessageProcessingDuration">The maximum amount of time a message should be allowed to be processed for.</param>
    /// <param name="handler">The handler function to process received messages.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the subscription.</param>
    /// <returns>A disposable subscription that can be disposed to unsubscribe.</returns>
    Task<IAsyncDisposable> SubscribeAsync(
        string pubSubName,
        string topicName,
        TimeSpan maxMessageProcessingDuration,
        TopicMessageHandler handler,
        CancellationToken cancellationToken = default);
}
