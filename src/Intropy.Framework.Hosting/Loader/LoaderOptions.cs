using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>A loader's subscription, routing and shutdown settings.</summary>
public sealed class LoaderOptions
{
    /// <summary>The Dapr pub/sub component to subscribe through. Required.</summary>
    public string PubSubName { get; set; } = "";

    /// <summary>The topic the loader consumes. Required.</summary>
    public string TopicName { get; set; } = "";

    /// <summary>Where the sidecar sends messages the loader drops: unrouted messages under
    /// <see cref="UnroutedPolicy.DeadLetter"/>, and messages the sidecar gives up redelivering.</summary>
    public string? DeadLetterTopic { get; set; }

    /// <summary>What a routing loader does with a message no route handles. Defaults to
    /// <see cref="UnroutedPolicy.DeadLetter"/>, which requires <see cref="DeadLetterTopic"/>.</summary>
    public UnroutedPolicy Unrouted { get; set; } = UnroutedPolicy.DeadLetter;

    /// <summary>How long one message may run before its pipeline is cancelled and the message is
    /// left for redelivery.</summary>
    public TimeSpan MaxMessageProcessingTime { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>How long the message in flight may finish after the host is asked to stop, before it
    /// is interrupted and left for redelivery. Keep it below the pod's
    /// <c>terminationGracePeriodSeconds</c>.</summary>
    public TimeSpan ShutdownGracePeriod { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>How long to keep trying to open the subscription (the sidecar may start after the
    /// app) before the loader gives up and stops the host with exit code 1. Streaming only.</summary>
    public TimeSpan SubscribeTimeout { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>How long to wait before reopening a subscription whose stream broke. Streaming
    /// only.</summary>
    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How the sidecar delivers the loader's messages. Defaults to
    /// <see cref="LoaderTransport.Streaming"/>. Experimental: see <see cref="LoaderTransport"/>.</summary>
    public LoaderTransport Transport { get; set; } = LoaderTransport.Streaming;

    /// <summary>The port the loader serves the Dapr gRPC app callback on under
    /// <see cref="LoaderTransport.AppCallback"/>: the sidecar's <c>app-port</c>. Defaults to
    /// <c>APP_PORT</c> from the environment, or 8080.</summary>
    public int? CallbackPort { get; set; }
}
