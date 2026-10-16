using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
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
    /// The name of the Dapr PubSub component to use. Optional: unset, it is the pub/sub the
    /// deployment names for the component, <see cref="FrameworkOptions.PubSubName"/>: for a
    /// transactional integration, the system's internal-queue pub/sub.
    /// </summary>
    public string DaprPubSubName { get; set; } = "";

    /// <summary>
    /// The name of the topic to use. Optional: unset, it is the topic the deployment names for the
    /// component, <see cref="FrameworkOptions.TopicName"/>: for a transactional integration, its
    /// internal queue's topic, which the system topology names <c>internal-&lt;component&gt;</c>. Keep it unique per
    /// integration: brokers name their exchanges, topics and queues after it, so integrations sharing
    /// a topic name would receive each other's files whatever Dapr component they publish through.
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
    /// When a payload is moved to the claim store instead of riding inside its message. Applies only
    /// when a claim store is registered (<c>AddClaimStore</c>); without one every payload stays inline,
    /// whatever this says.
    /// </summary>
    /// <remarks>
    /// This governs <em>writing</em> claims only. The send side resolves a pointer whenever a message
    /// carries one, whatever the mode, so <see cref="Blocks.Shared.ClaimCheckMode.Never"/> stops new
    /// claims while leaving existing ones to drain — see that mode for the rollback order.
    /// </remarks>
    /// <value>Default: <see cref="Blocks.Shared.ClaimCheckMode.Auto"/>, which requires
    /// <see cref="ClaimCheckThresholdBytes"/>.</value>
    public ClaimCheckMode ClaimCheckMode { get; set; } = ClaimCheckMode.Auto;

    /// <summary>
    /// The largest encoded message that may be published inline, in bytes. Required when a claim store
    /// is registered in <see cref="Blocks.Shared.ClaimCheckMode.Auto"/>, and ignored in
    /// <see cref="Blocks.Shared.ClaimCheckMode.Always"/> and
    /// <see cref="Blocks.Shared.ClaimCheckMode.Never"/>.
    /// </summary>
    /// <remarks>
    /// Set it 1–2 KB below the broker's maximum message size rather than at it. The measurement is
    /// taken before the trace context is attached to the envelope, and Dapr adds attributes of its own
    /// (<c>topic</c>, <c>pubsubname</c>, <c>traceid</c>) when it forwards the event, so the message the
    /// broker is handed is always somewhat larger than what the threshold was compared against.
    /// </remarks>
    public int? ClaimCheckThresholdBytes { get; set; }

    /// <summary>The claim-check policy these options describe, or <see langword="null"/> in
    /// <see cref="Blocks.Shared.ClaimCheckMode.Never"/>: nothing is claim-checked, so the step is left
    /// out of the pipeline rather than carried as a no-op.</summary>
    /// <exception cref="InvalidOperationException">The mode needs a threshold and none is set.</exception>
    internal ClaimCheckPolicy? ClaimCheckPolicy() => ClaimCheckMode switch
    {
        ClaimCheckMode.Never => null,
        ClaimCheckMode.Always => Blocks.Shared.ClaimCheckPolicy.Always,
        _ => Blocks.Shared.ClaimCheckPolicy.Above(ClaimCheckThresholdBytes ?? throw new InvalidOperationException(
            $"{nameof(TransactionalIntegrationOptions)}.{nameof(ClaimCheckThresholdBytes)} must be set when a claim " +
            $"store is registered and {nameof(ClaimCheckMode)} is {ClaimCheckMode.Auto}: it is the " +
            "broker's maximum message size, which the framework cannot know. Set it below that limit with room to " +
            $"spare, or use {nameof(ClaimCheckMode)} {ClaimCheckMode.Always} or " +
            $"{ClaimCheckMode.Never}."))
    };

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
    /// generates for the component <paramref name="framework"/> names, in both shapes.</summary>
    /// <exception cref="InvalidOperationException">The pub/sub or the topic is unset and so is the
    /// <see cref="FrameworkOptions"/> value it defaults from.</exception>
    internal void UseInternalQueueDefaults(FrameworkOptions framework)
    {
        if (string.IsNullOrEmpty(DaprPubSubName))
            DaprPubSubName = !string.IsNullOrEmpty(framework.PubSubName)
                ? framework.PubSubName
                : throw new InvalidOperationException(
                    $"{nameof(TransactionalIntegrationOptions)}.{nameof(DaprPubSubName)} is unset and so is the " +
                    $"pub/sub it defaults from: set the INTROPY_PUBSUB_NAME environment variable, " +
                    $"{nameof(FrameworkOptions)}.{nameof(FrameworkOptions.PubSubName)}, or {nameof(DaprPubSubName)}.");
        if (string.IsNullOrEmpty(DaprTopicName))
            DaprTopicName = !string.IsNullOrEmpty(framework.TopicName)
                ? framework.TopicName
                : throw new InvalidOperationException(
                    $"{nameof(TransactionalIntegrationOptions)}.{nameof(DaprTopicName)} is unset and so is the " +
                    $"topic it defaults from: set the INTROPY_TOPIC_NAME environment variable, " +
                    $"{nameof(FrameworkOptions)}.{nameof(FrameworkOptions.TopicName)}, or {nameof(DaprTopicName)}.");
        _subscription.PubSubName = DaprPubSubName;
        _subscription.TopicName = DaprTopicName;
    }

    /// <summary>Checks that a threshold, when set, is a usable size.</summary>
    /// <remarks>Only the range is checked here. Whether a threshold is <em>required</em> depends on
    /// the mode and on a claim store being registered, and the store may be registered either side of
    /// the integration, so that check stays in <see cref="ClaimCheckPolicy"/> where both are
    /// known.</remarks>
    internal void EnsureValidClaimCheckThreshold(string paramName)
    {
        if (ClaimCheckThresholdBytes is { } bytes && bytes <= 0)
            throw new ArgumentOutOfRangeException(paramName, bytes,
                $"{nameof(TransactionalIntegrationOptions)}.{nameof(ClaimCheckThresholdBytes)} must be positive when " +
                "set: it is the largest message that may be published inline, so a zero or negative limit would " +
                "claim-check everything or nothing. Leave it unset to let the mode decide whether one is needed.");
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
