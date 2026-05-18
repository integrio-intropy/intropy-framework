using Dapr.Messaging.PublishSubscribe;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;

/// <summary>
/// Dapr implementation of <see cref="ITopicSubscriber"/>.
/// </summary>
/// <param name="daprPublishSubscribeClient">The Dapr pub/sub client.</param>
public class DaprTopicSubscriber(DaprPublishSubscribeClient daprPublishSubscribeClient) : ITopicSubscriber
{
    /// <inheritdoc/>
    public Task<IAsyncDisposable> SubscribeAsync(
        string pubSubName,
        string topicName,
        TimeSpan maxMessageProcessingDuration,
        TopicMessageHandler handler,
        CancellationToken cancellationToken = default)
    {
        var subscriptionOptions = new DaprSubscriptionOptions(
            new MessageHandlingPolicy(
                maxMessageProcessingDuration,
                TopicResponseAction.Retry));

        return daprPublishSubscribeClient.SubscribeAsync(
            pubSubName,
            topicName,
            subscriptionOptions,
            handler,
            cancellationToken);
    }
}
