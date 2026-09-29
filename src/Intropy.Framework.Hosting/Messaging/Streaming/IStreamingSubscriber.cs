using Dapr.Messaging.PublishSubscribe;

namespace Intropy.Framework.Hosting.Messaging.Streaming;

/// <summary>
/// Opens a Dapr streaming subscription: the app connects to its sidecar and the sidecar delivers
/// the topic's messages over that connection, so the app serves no HTTP and needs no app port.
/// The seam between a loader and the sidecar — tests register a fake that delivers messages
/// directly.
/// </summary>
public interface IStreamingSubscriber
{
    /// <summary>Subscribes <paramref name="handler"/> to <paramref name="topicName"/>.</summary>
    /// <param name="pubSubName">The Dapr pub/sub component.</param>
    /// <param name="topicName">The topic to consume.</param>
    /// <param name="options">The subscription's options: message handling policy, dead-letter
    /// topic, error handler.</param>
    /// <param name="handler">Handles each message; its result is the message's acknowledgement.</param>
    /// <param name="cancellationToken">Cancels opening the subscription.</param>
    /// <returns>The open subscription. Its <see cref="IDaprSubscription.Completion"/> ends when the
    /// stream to the sidecar does; disposing it closes the stream.</returns>
    Task<IDaprSubscription> SubscribeAsync(string pubSubName, string topicName, DaprSubscriptionOptions options,
        TopicMessageHandler handler, CancellationToken cancellationToken);
}

/// <summary>Subscribes through the Dapr SDK's streaming subscription client.</summary>
internal sealed class DaprStreamingSubscriber(DaprPublishSubscribeClient client) : IStreamingSubscriber
{
    public async Task<IDaprSubscription> SubscribeAsync(string pubSubName, string topicName,
        DaprSubscriptionOptions options, TopicMessageHandler handler, CancellationToken cancellationToken)
    {
        var subscription = await client.SubscribeAsync(pubSubName, topicName, options, handler, cancellationToken);
        if (subscription is IDaprSubscription daprSubscription)
            return daprSubscription;

        // Every Dapr.Messaging release since 1.18.10 returns one; without it a broken stream
        // could go unnoticed.
        await subscription.DisposeAsync();
        throw new InvalidOperationException(
            $"The Dapr subscription client returned a {subscription.GetType().Name}, not an {nameof(IDaprSubscription)}.");
    }
}
