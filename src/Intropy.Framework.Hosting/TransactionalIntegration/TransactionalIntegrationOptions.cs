using Intropy.Framework.Hosting.Common;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// Configuration options for the Transactional Integration. The subscription concerns (message
/// processing time, callback port) live on the composed <see cref="Subscription"/>; this class's
/// own members delegate to it. The pub/sub and topic names keep their historical members
/// (<see cref="DaprPubSubName"/>, <see cref="DaprTopicName"/>) and can additionally be set through
/// <see cref="Subscription"/>: the two shapes are one value each, checked at registration.
/// </summary>
public class TransactionalIntegrationOptions
{
    private readonly SubscriptionOptions _subscription = new() { MaxMessageProcessingTime = TimeSpan.FromSeconds(40) };

    /// <summary>The integration's subscription: pub/sub, topic, message processing time and
    /// callback port. An additional way to set the values this class's own members carry:
    /// <see cref="DaprPubSubName"/> and <see cref="SubscriptionOptions.PubSubName"/> are the same
    /// value, as are <see cref="DaprTopicName"/> and <see cref="SubscriptionOptions.TopicName"/>
    /// and the <see cref="MaxMessageProcessingTime"/> and <see cref="CallbackPort"/> delegations.
    /// Setting the same value in both shapes with different values fails at registration.</summary>
    public SubscriptionOptions Subscription => _subscription;

    /// <summary>
    /// The name of the Dapr PubSub component to use. Optional: unset, it is the internal queue's
    /// pub/sub the system topology generates for the component, <c>internal-&lt;component&gt;</c>
    /// (the component name with dots as dashes).
    /// </summary>
    public string DaprPubSubName { get; set; } = "";

    /// <summary>
    /// The name of the topic to use. Optional: unset, it is the internal queue's topic the system
    /// topology generates for the component, <c>hop</c>.
    /// </summary>
    public string DaprTopicName { get; set; } = "";

    /// <summary>The internal queue's topic the system topology generates for every transactional
    /// integration.</summary>
    internal const string InternalQueueTopicName = "hop";

    /// <summary>The internal queue's pub/sub the system topology generates for
    /// <paramref name="componentName"/>: scoped to the component alone, named after its app id
    /// (the component name with dots as dashes).</summary>
    internal static string InternalQueuePubSubName(string componentName) =>
        $"internal-{componentName.Replace('.', '-')}";


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
    /// The longest a run waits, before it sweeps, for the internal queue to deliver a probe. A
    /// broker can discard a message published before the subscription has created its queue, so
    /// no file is published (and completed) until a probe from this run has come back. When none
    /// comes back in time, the run touches no source file and exits with
    /// <c>JobExitCodes.InfrastructureFailure</c>. Must be positive and within the .NET timer range.
    /// Applies only when the receive side's enqueuer can be probed (the built-in Dapr enqueuer).
    /// Run only one job instance per internal queue; competing consumers can receive each other's
    /// probes. This startup check does not protect against queue deletion after readiness.
    /// </summary>
    /// <value>Default: 60 seconds</value>
    public TimeSpan InternalQueueReadyTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>How long a run waits for a probe before publishing another.</summary>
    internal TimeSpan InternalQueueProbeInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The max amount of time a message is allowed to be processed for.
    /// It is recommended to set this lower than <see cref="PostIdleGracePeriod"/>.
    /// Delegates to <see cref="SubscriptionOptions.MaxMessageProcessingTime"/> on
    /// <see cref="Subscription"/>.
    /// </summary>
    /// <value>Default: 40 seconds</value>
    public TimeSpan MaxMessageProcessingTime
    {
        get => _subscription.MaxMessageProcessingTime;
        set => _subscription.MaxMessageProcessingTime = value;
    }

    /// <summary>
    /// The port the integration serves the Dapr gRPC app callback on, through which the sidecar
    /// pushes its queue: the sidecar's <c>app-port</c>, with <c>app-protocol</c> <c>grpc</c>.
    /// Defaults to <c>APP_PORT</c> from the environment, or 8080. Delegates to
    /// <see cref="SubscriptionOptions.CallbackPort"/> on <see cref="Subscription"/>.
    /// </summary>
    public int? CallbackPort
    {
        get => _subscription.CallbackPort;
        set => _subscription.CallbackPort = value;
    }

    /// <summary>Settles the subscription values across the two shapes: a value set through
    /// <see cref="Subscription"/> but not through this class's own member fills that member, and
    /// the other way around, so both shapes read the same values after registration. Called by the
    /// registration after option configuration and definition validation.</summary>
    /// <exception cref="InvalidOperationException">A pub/sub or topic name is set in both shapes
    /// with different values.</exception>
    internal void ConsolidateSubscription()
    {
        DaprPubSubName = Consolidate(nameof(DaprPubSubName), DaprPubSubName, nameof(SubscriptionOptions.PubSubName),
            _subscription.PubSubName);
        DaprTopicName = Consolidate(nameof(DaprTopicName), DaprTopicName, nameof(SubscriptionOptions.TopicName),
            _subscription.TopicName);
        _subscription.PubSubName = DaprPubSubName;
        _subscription.TopicName = DaprTopicName;
    }

    /// <summary>Fills the pub/sub and topic left unset with the internal queue the system topology
    /// generates for <paramref name="componentName"/>, in both shapes.</summary>
    internal void UseInternalQueueDefaults(string componentName)
    {
        if (string.IsNullOrEmpty(DaprPubSubName))
            DaprPubSubName = InternalQueuePubSubName(componentName);
        if (string.IsNullOrEmpty(DaprTopicName))
            DaprTopicName = InternalQueueTopicName;
        _subscription.PubSubName = DaprPubSubName;
        _subscription.TopicName = DaprTopicName;
    }

    /// <summary>Checks that readiness always has a bounded, usable .NET timer.</summary>
    internal void EnsureValidInternalQueueReadyTimeout(string paramName)
    {
        if (InternalQueueReadyTimeout <= TimeSpan.Zero || InternalQueueReadyTimeout.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(paramName, InternalQueueReadyTimeout,
                $"{nameof(TransactionalIntegrationOptions)}.{nameof(InternalQueueReadyTimeout)} must be positive and " +
                $"no greater than {uint.MaxValue - 1} milliseconds.");
    }

    private static string Consolidate(string legacyMember, string legacy, string composedMember, string composed)
    {
        if (!string.IsNullOrEmpty(legacy) && !string.IsNullOrEmpty(composed) &&
            !string.Equals(legacy, composed, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{legacyMember} and Subscription.{composedMember} disagree ('{legacy}' vs '{composed}'): " +
                "the two shapes carry the same value; set it through one of them.");
        if (string.IsNullOrEmpty(legacy))
            return composed;
        return legacy;
    }
}
