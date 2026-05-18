namespace Intropy.Framework.Hosting.TransactionalIntegration.Job;

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
    /// <value>Default: 5</value>
    public int IdleTimeoutSeconds { get; set; } = 5;

    /// <summary>
    /// The time to wait for in-flight messages to finish processing,
    /// after the <see cref="IdleTimeoutSeconds"/> has been reached.
    /// </summary>
    /// <value>Default: 45</value>
    public int PostIdleGracePeriodSeconds { get; set; } = 45;

    /// <summary>
    /// The max amount of time a message is allowed to be processed for.
    /// It is recommended to set this lower than <see cref="PostIdleGracePeriodSeconds"/>.
    /// </summary>
    /// <value>Default: 40</value>
    public int MaxMessageProcessingTimeSeconds { get; set; } = 40;

    /// <summary>
    /// The maximum time to wait for the Dapr sidecar to become available, in seconds.
    /// </summary>
    /// <value>Default: 30</value>
    public int SidecarTimeoutSeconds { get; set; } = 30;


    /// <summary>
    /// Creates new instance of <see cref="TransactionalIntegrationOptions"/>
    /// </summary>
    /// <param name="daprPubSubName"></param>
    /// <param name="daprTopicName"></param>
    public TransactionalIntegrationOptions(string daprPubSubName, string daprTopicName)
    {
        DaprPubSubName = daprPubSubName;
        DaprTopicName = daprTopicName;
    }

    /// <summary>
    /// Creates new instance of <see cref="TransactionalIntegrationOptions"/>
    /// </summary>
    public TransactionalIntegrationOptions()
    {
    }
}
