using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>A loader's subscription, routing, callback and shutdown settings.</summary>
public sealed class LoaderOptions
{
    /// <summary>The Dapr pub/sub component to subscribe through. Required.</summary>
    public string PubSubName { get; set; } = "";

    /// <summary>The topic the loader consumes. Required.</summary>
    public string TopicName { get; set; } = "";

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
    /// left for redelivery.</summary>
    public TimeSpan MaxMessageProcessingTime { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>How long the message in flight may finish after the host is asked to stop, before it
    /// is interrupted and left for redelivery. Keep it below the pod's
    /// <c>terminationGracePeriodSeconds</c>.</summary>
    public TimeSpan ShutdownGracePeriod { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>The port the loader serves the Dapr gRPC app callback on: the sidecar's
    /// <c>app-port</c>, with <c>app-protocol</c> <c>grpc</c>. Defaults to <c>APP_PORT</c> from the
    /// environment, or 8080.</summary>
    public int? CallbackPort { get; set; }
}
