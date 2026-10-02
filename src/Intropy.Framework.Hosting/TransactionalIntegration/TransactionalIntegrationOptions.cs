namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// Configuration options for the Transactional Integration
/// </summary>
public class TransactionalIntegrationOptions
{
    /// <summary>
    /// The name of the Dapr PubSub component to use.
    /// </summary>
    public string DaprPubSubName { get; set; } = "";

    /// <summary>
    /// The name of the topic to use.
    /// </summary>
    public string DaprTopicName { get; set; } = "";


    /// <summary>
    /// The time to wait between messages before deciding to shut down.
    /// This time begins when the adapter has returned all files, and they have been published to the queue.
    /// </summary>
    /// <value>Default: 5 seconds</value>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The time to wait for in-flight messages to finish processing,
    /// after the <see cref="IdleTimeout"/> has been reached.
    /// </summary>
    /// <value>Default: 45 seconds</value>
    public TimeSpan PostIdleGracePeriod { get; set; } = TimeSpan.FromSeconds(45);

    /// <summary>
    /// The max amount of time a message is allowed to be processed for.
    /// It is recommended to set this lower than <see cref="PostIdleGracePeriod"/>.
    /// </summary>
    /// <value>Default: 40 seconds</value>
    public TimeSpan MaxMessageProcessingTime { get; set; } = TimeSpan.FromSeconds(40);

    /// <summary>
    /// The port the integration serves the Dapr gRPC app callback on, through which the sidecar
    /// pushes its queue: the sidecar's <c>app-port</c>, with <c>app-protocol</c> <c>grpc</c>.
    /// Defaults to <c>APP_PORT</c> from the environment, or 8080.
    /// </summary>
    public int? CallbackPort { get; set; }

    /// <summary>
    /// Creates new instance of <see cref="TransactionalIntegrationOptions"/>
    /// </summary>
    public TransactionalIntegrationOptions()
    {
    }
}
