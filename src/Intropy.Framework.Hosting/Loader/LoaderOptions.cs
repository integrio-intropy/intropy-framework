using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>A loader's subscription, routing, callback and shutdown settings. The subscription
/// concerns (pub/sub, topic, message processing time, callback port) live on the composed
/// <see cref="Subscription"/>; this class's own members delegate to it, so they can be set through
/// either shape and always end up the same values.</summary>
public sealed class LoaderOptions
{
    /// <summary>The loader's subscription: the pub/sub, the topic, the message processing time and
    /// the app-callback port — the <see cref="PubSubName"/>, <see cref="TopicName"/>,
    /// <see cref="MaxMessageProcessingTime"/> and <see cref="CallbackPort"/> members of this class
    /// delegate to it, so setting through one shape shows through the other.</summary>
    public SubscriptionOptions Subscription { get; } = new() { MaxMessageProcessingTime = TimeSpan.FromMinutes(1) };

    /// <summary>The Dapr pub/sub component the loader expects deliveries from. Optional: the
    /// component's Dapr Subscription resource decides what is delivered; when set, a delivery from
    /// another pub/sub is refused as a mismatch. Delegates to
    /// <see cref="SubscriptionOptions.PubSubName"/> on <see cref="Subscription"/>.</summary>
    public string PubSubName
    {
        get => Subscription.PubSubName;
        set => Subscription.PubSubName = value;
    }

    /// <summary>The topic the loader expects deliveries from. Optional: the component's Dapr
    /// Subscription resource decides what is delivered; when set, a delivery from another topic is
    /// refused as a mismatch. Delegates to
    /// <see cref="SubscriptionOptions.TopicName"/> on <see cref="Subscription"/>.</summary>
    public string TopicName
    {
        get => Subscription.TopicName;
        set => Subscription.TopicName = value;
    }

    /// <summary>What a routing loader does with a message no route handles. Defaults to
    /// <see cref="UnroutedPolicy.DeadLetter"/>.</summary>
    /// <remarks>
    /// A loader dead-letters through the broker's own dead-letter queue (Azure Service Bus's
    /// <c>$deadletterqueue</c>, RabbitMQ's with the component's <c>enableDeadLetter</c>), so it
    /// subscribes without a Dapr dead-letter topic and never answers <c>DROP</c>, which the sidecar
    /// would discard. A message it leaves for redelivery is retried under the Dapr
    /// <c>Resiliency</c> policy on the pub/sub's inbound deliveries, if one is deployed, and then
    /// handed back to the broker, which dead-letters it.
    /// </remarks>
    public UnroutedPolicy Unrouted { get; set; } = UnroutedPolicy.DeadLetter;

    /// <summary>How long one message may run before its pipeline is cancelled and the message is
    /// left for redelivery. Delegates to
    /// <see cref="SubscriptionOptions.MaxMessageProcessingTime"/> on <see cref="Subscription"/>.</summary>
    public TimeSpan MaxMessageProcessingTime
    {
        get => Subscription.MaxMessageProcessingTime;
        set => Subscription.MaxMessageProcessingTime = value;
    }

    /// <summary>How long the message in flight may finish after the host is asked to stop, before it
    /// is interrupted and left for redelivery. Keep it below the pod's
    /// <c>terminationGracePeriodSeconds</c>.</summary>
    public TimeSpan ShutdownGracePeriod { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>The port the loader serves the Dapr gRPC app callback on: the sidecar's
    /// <c>app-port</c>, with <c>app-protocol</c> <c>grpc</c>. Defaults to <c>APP_PORT</c> from the
    /// environment, or 8080. Delegates to <see cref="SubscriptionOptions.CallbackPort"/> on
    /// <see cref="Subscription"/>.</summary>
    public int? CallbackPort
    {
        get => Subscription.CallbackPort;
        set => Subscription.CallbackPort = value;
    }
}
