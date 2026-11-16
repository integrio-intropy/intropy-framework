using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>A bulk loader's subscription, routing, callback and shutdown settings. The subscription
/// concerns (pub/sub, topic, batch processing time, callback port) live on the composed
/// <see cref="Subscription"/>; this class's own members delegate to it, so they can be set through
/// either shape and always end up the same values.</summary>
/// <remarks>
/// How many messages a batch carries and how long the sidecar waits to fill one are the
/// subscription resource's (<c>bulkSubscribe.maxMessagesCount</c>,
/// <c>bulkSubscribe.maxAwaitDurationMs</c>), not the block's: the block takes whatever batch
/// arrives. How much of a batch runs at once is the pipeline's, through
/// <c>BulkLoaderBuilder.WithParallelism</c>.
/// </remarks>
public sealed class BulkLoaderOptions
{
    /// <summary>The bulk loader's subscription: the pub/sub, the topic, the batch processing time
    /// and the app-callback port — the <see cref="PubSubName"/>, <see cref="TopicName"/>,
    /// <see cref="MaxBatchProcessingTime"/> and <see cref="CallbackPort"/> members of this class
    /// delegate to it, so setting through one shape shows through the other.</summary>
    public SubscriptionOptions Subscription { get; } = new() { MaxMessageProcessingTime = TimeSpan.FromMinutes(5) };

    /// <summary>The Dapr pub/sub component the loader expects deliveries from. Optional: the
    /// component's Dapr Subscription resource decides what is delivered; when set, a delivery from
    /// another pub/sub is refused as a mismatch. Unset, it is the pub/sub the deployment names for
    /// the component, <see cref="FrameworkOptions.PubSubName"/>, if any.</summary>
    public string PubSubName
    {
        get => Subscription.PubSubName;
        set => Subscription.PubSubName = value;
    }

    /// <summary>The topic the loader expects deliveries from. Optional: the component's Dapr
    /// Subscription resource decides what is delivered; when set, a delivery from another topic is
    /// refused as a mismatch. Unset, it is the topic the deployment names for the component,
    /// <see cref="FrameworkOptions.TopicName"/>, if any.</summary>
    public string TopicName
    {
        get => Subscription.TopicName;
        set => Subscription.TopicName = value;
    }

    /// <summary>What a routing bulk loader does with a batch no route handles. Defaults to
    /// <see cref="UnroutedPolicy.DeadLetter"/>, like a single-message loader's.</summary>
    public UnroutedPolicy Unrouted { get; set; } = UnroutedPolicy.DeadLetter;

    /// <summary>How long one whole batch may run before its pipeline is cancelled and every message
    /// in it is left for redelivery. A batch is one unit of work, so this covers all of its
    /// messages together — which is why it defaults higher than a single message's limit.</summary>
    public TimeSpan MaxBatchProcessingTime
    {
        get => Subscription.MaxMessageProcessingTime;
        set => Subscription.MaxMessageProcessingTime = value;
    }

    /// <summary>How long a batch in flight may finish after the host is asked to stop, before it is
    /// interrupted and left for redelivery. Keep it below the pod's
    /// <c>terminationGracePeriodSeconds</c>.</summary>
    public TimeSpan ShutdownGracePeriod { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>The port the loader serves the Dapr gRPC app callback on: the sidecar's
    /// <c>app-port</c>, with <c>app-protocol</c> <c>grpc</c>. Defaults to <c>APP_PORT</c> from the
    /// environment, or 8080.</summary>
    public int? CallbackPort
    {
        get => Subscription.CallbackPort;
        set => Subscription.CallbackPort = value;
    }

    /// <summary>Fills the pub/sub and topic left unset with the ones the deployment names for the
    /// component, so the loader refuses deliveries from any other subscription. Values set in code
    /// or configuration win; a name the deployment does not give stays unset.</summary>
    internal void UseDeploymentDefaults(FrameworkOptions framework)
    {
        if (string.IsNullOrEmpty(PubSubName) && !string.IsNullOrEmpty(framework.PubSubName))
            PubSubName = framework.PubSubName;
        if (string.IsNullOrEmpty(TopicName) && !string.IsNullOrEmpty(framework.TopicName))
            TopicName = framework.TopicName;
    }
}
