namespace Intropy.Framework.Components;

/// <summary>
/// A component's identity: what its <see cref="IntropyComponent.Manifest"/> declares, and what
/// <see cref="ComponentHost"/> and the test hosts assemble the component from. The members are
/// checked as they are set — a blank member, a relative event source — so a manifest that exists
/// is a usable one.
/// </summary>
/// <remarks>
/// The channel is not part of the identity: a component runs on the pub/sub and topic the topology
/// settles (<c>INTROPY_PUBSUB_NAME</c>, <c>INTROPY_TOPIC_NAME</c>), so a topic the system later
/// changes cannot leave a stale name in the component. Nor is the payload: it is the component's
/// type parameter (e.g. <c>GatewayComponent&lt;OrderPlaced&gt;</c>), so the compiler holds every
/// route to it.
/// </remarks>
public sealed class ComponentManifest
{
    /// <summary>The component's logical name: the telemetry service name and the prefix of the
    /// component's idempotency and incident records.</summary>
    public required string ComponentName { get; init => field = NotBlank(value, nameof(ComponentName)); }

    /// <summary>The organization the component runs for: the telemetry service namespace.</summary>
    public required string Organization { get; init => field = NotBlank(value, nameof(Organization)); }

    /// <summary>The message the component publishes: the CloudEvents type of every event it sends.</summary>
    public required string Publishes { get; init => field = NotBlank(value, nameof(Publishes)); }

    /// <summary>The CloudEvent <c>source</c> of every event the component sends, as an absolute URI
    /// (e.g. <c>urn:Acme:order-gateway</c>).</summary>
    public required string EventSource
    {
        get;
        init => field = Uri.TryCreate(NotBlank(value, nameof(EventSource)), UriKind.Absolute, out _)
            ? value
            : throw new ArgumentException(
                $"EventSource '{value}' is not an absolute URI (e.g. urn:Acme:order-gateway): the source is the " +
                "CloudEvent source of every event the component sends.", nameof(EventSource));
    }

    /// <summary>The port the component reaches the outside world through: the endpoint the system
    /// declares for it. A gateway must expose this port, at <see cref="EndpointPath"/>; a block
    /// without an outside world leaves it unset.</summary>
    public string? Port { get; init; }

    /// <summary>The path callers reach the system-declared endpoint on, starting with <c>/</c>.
    /// Default: <c>/</c> followed by <see cref="Publishes"/>.</summary>
    public string? EndpointPath { get; init; }

    private static string NotBlank(string value, string member) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new ArgumentException(
                $"The component manifest's {member} is blank: the member is required, and the framework cannot " +
                "name the component without it.", member)
            : value;
}
