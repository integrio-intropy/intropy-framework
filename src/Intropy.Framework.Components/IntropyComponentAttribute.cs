namespace Intropy.Framework.Components;

/// <summary>
/// A component's identity, declared in code. Put it on the component's class — the
/// <see cref="IntropyComponent"/> subclass — and the framework reads it at startup; a source
/// generator shipped in this package turns it into the scaffold record (<c>.intropy/scaffold.json</c>,
/// schema version 1) that <c>intropy sys create</c> assembles the system from. The code names the
/// component once; the record is a build artifact of that naming, so the component, the system
/// declaration and the deployment cannot drift apart.
/// </summary>
/// <remarks>
/// <code>
/// [IntropyComponent("order-gateway", Organization = "Acme", Publishes = "order-placed",
///     Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
///     EventSource = "urn:Acme:order-gateway")]
/// public sealed class OrderGateway : GatewayComponent { ... }
/// </code>
/// <para>
/// The generator checks the attribute at compile time: a missing member, or an
/// <see cref="EventSource"/> that is not an absolute URI, fails the build. The same checks run at
/// startup for a manifest the host assembled by hand. The generator also fails the build when a
/// concrete <see cref="IntropyComponent"/> subclass carries no attribute at all.
/// </para>
/// <para>
/// The channel is not part of the identity. A component runs on the pub/sub and topic the topology
/// settles, which the system host and the deployment pass in <c>INTROPY_PUBSUB_NAME</c> and
/// <c>INTROPY_TOPIC_NAME</c>; the record keeps no channel either.
/// </para>
/// </remarks>
/// <param name="componentName">The component's logical name: the telemetry service name and the
/// prefix of its idempotency and incident records. The record's <c>values.componentName</c>.</param>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class IntropyComponentAttribute(string componentName) : Attribute
{
    /// <summary>The component's logical name: the telemetry service name and the prefix of its
    /// idempotency and incident records. The record's <c>values.componentName</c>.</summary>
    public string ComponentName { get; } = componentName;

    /// <summary>The organization the component runs for: the telemetry service namespace. The
    /// record's <c>values.organization</c>.</summary>
    public required string Organization { get; init; }

    /// <summary>The message the component publishes: the CloudEvents type of every event it sends.
    /// The record's <c>values.publishes</c>.</summary>
    public required string Publishes { get; init; }

    /// <summary>The payload the message projects to, typed: every route that publishes the
    /// component's message must name this type as its <c>TOutput</c>, checked at startup. The
    /// record's <c>values.payloadType</c> — the payload type's fully qualified name.</summary>
    public required Type Payload { get; init; }

    /// <summary>The CloudEvent <c>source</c> of every event the component sends, as an absolute URI
    /// (e.g. <c>urn:Acme:order-gateway</c>). The record's <c>values.eventSourceValue</c>.</summary>
    public required string EventSource { get; init; }

    /// <summary>The port the component reaches the outside world through: the endpoint the system
    /// declares for it. A gateway's <see cref="Gateway.GatewayComponent"/> must expose this port,
    /// at <see cref="EndpointPath"/>; a block without an outside world leaves it unset. The
    /// record's <c>values.port</c>.</summary>
    public string? Port { get; init; }

    /// <summary>The path callers reach the system-declared endpoint on, starting with <c>/</c>.
    /// Default: <c>/</c> followed by <see cref="Publishes"/>. The record's
    /// <c>values.endpointPath</c>.</summary>
    public string? EndpointPath { get; init; }
}
