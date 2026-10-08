namespace Intropy.Framework.Hosting.Common;

/// <summary>The subscription concerns every component that consumes one Dapr topic shares: the
/// pub/sub, the topic, the message processing time and the app-callback port. The component options
/// (<see cref="Loader.LoaderOptions"/>, <see cref="TransactionalIntegration.TransactionalIntegrationOptions"/>)
/// compose one instance, and their own members delegate to it, so a subscription can be shaped
/// either through the options' own members — through whichever name the component documents — or
/// through this one object. Both always end up the same values.</summary>
public sealed class SubscriptionOptions
{
    /// <summary>The Dapr pub/sub component the subscription is on. Empty means any: the component
    /// takes what its Dapr Subscription resource delivers. Whether it is required is the
    /// component's to say.</summary>
    public string PubSubName { get; set; } = "";

    /// <summary>The topic the component consumes. Empty means any: the component takes what its
    /// Dapr Subscription resource delivers. Whether it is required is the component's to
    /// say.</summary>
    public string TopicName { get; set; } = "";

    /// <summary>How long one message may run before its pipeline is cancelled and the message is
    /// left for redelivery.</summary>
    public TimeSpan MaxMessageProcessingTime { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>The port the component serves the Dapr gRPC app callback on: the sidecar's
    /// <c>app-port</c>, with <c>app-protocol</c> <c>grpc</c>. Defaults to <c>APP_PORT</c> from the
    /// environment, or 8080.</summary>
    public int? CallbackPort { get; set; }
}
