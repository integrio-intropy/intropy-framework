using System.Reflection;

namespace Intropy.Framework.Components;

/// <summary>
/// A component's identity, declared on its class with <see cref="IntropyComponentAttribute"/>: the
/// attribute <see cref="ComponentManifest.From(Type)"/> reads and <see cref="ComponentHost"/> and the
/// test hosts assemble the component from. The source generator shipped in this package turns the
/// same attribute into the scaffold record (<c>.intropy/scaffold.json</c>, schema version 1) that
/// <c>intropy sys create</c> assembles the system from, so the code, the system declaration and
/// the deployment all name the component from one declaration.
/// </summary>
/// <remarks>
/// The channel is not part of the identity. The attribute declares no channel, and the record
/// keeps none: a component runs on the pub/sub and topic the topology settles
/// (<c>INTROPY_PUBSUB_NAME</c>, <c>INTROPY_TOPIC_NAME</c>), so a topic the system later changes
/// cannot leave a stale name in the component.
/// </remarks>
public sealed class ComponentManifest
{
    /// <summary>The component's logical name: the telemetry service name and the prefix of the
    /// component's idempotency and incident records. Attribute <see cref="IntropyComponentAttribute.ComponentName"/>.</summary>
    public required string ComponentName { get; init; }

    /// <summary>The organization the component runs for: the telemetry service namespace.
    /// Attribute <see cref="IntropyComponentAttribute.Organization"/>.</summary>
    public required string Organization { get; init; }

    /// <summary>The message the component publishes: the CloudEvents type of every event it sends.
    /// Attribute <see cref="IntropyComponentAttribute.Publishes"/>.</summary>
    public string? Publishes { get; init; }

    /// <summary>The payload the message projects to: every route publishing the component's
    /// message must name it as its <c>TOutput</c>. Attribute
    /// <see cref="IntropyComponentAttribute.Payload"/>.</summary>
    public Type? Payload { get; init; }

    /// <summary>The port the component reaches the outside world through — the endpoint a gateway
    /// exposes. Attribute <see cref="IntropyComponentAttribute.Port"/>.</summary>
    public string? Port { get; init; }

    /// <summary>The path a gateway accepts items on. Attribute
    /// <see cref="IntropyComponentAttribute.EndpointPath"/>.</summary>
    public string? EndpointPath { get; init; }

    /// <summary>The CloudEvent <c>source</c> of every event the component sends. Attribute
    /// <see cref="IntropyComponentAttribute.EventSource"/>.</summary>
    public string? EventSource { get; init; }

    /// <summary>Reads the identity <paramref name="componentType"/> declares with its
    /// <see cref="IntropyComponentAttribute"/>, checked: every required member set, the event
    /// source an absolute URI. The noisy checks — a missing member, a relative source — already
    /// fail the build; the component's host runs them again, so a manifest assembled by hand
    /// cannot sneak an incomplete identity past startup.</summary>
    /// <param name="componentType">The component's class, carrying the attribute.</param>
    /// <returns>The component's manifest.</returns>
    /// <exception cref="InvalidOperationException">The class carries no
    /// <see cref="IntropyComponentAttribute"/>, a required member is missing, or the event source
    /// is not an absolute URI.</exception>
    public static ComponentManifest From(Type componentType)
    {
        ArgumentNullException.ThrowIfNull(componentType);
        var attribute = componentType.GetCustomAttribute<IntropyComponentAttribute>()
            ?? throw new InvalidOperationException(
                $"{componentType.Name} carries no IntropyComponent attribute: a component declares its identity in " +
                "code — put [IntropyComponent(...)] on its class, with its name, organization, message, payload " +
                "and event source.");
        return From(componentType.Name, attribute);
    }

    /// <summary>Checks an attribute and turns it into a manifest. The check names the component the
    /// attribute sits on, so an error says whose identity is incomplete.</summary>
    /// <param name="componentName">The class's name, for error messages.</param>
    /// <param name="attribute">The identity the class declares.</param>
    /// <returns>The component's manifest.</returns>
    /// <exception cref="InvalidOperationException">A required member is missing, or the event
    /// source is not an absolute URI.</exception>
    internal static ComponentManifest From(string componentName, IntropyComponentAttribute attribute)
    {
        ArgumentNullException.ThrowIfNull(attribute);
        if (string.IsNullOrWhiteSpace(attribute.ComponentName))
            throw Missing(componentName, nameof(IntropyComponentAttribute.ComponentName));
        if (string.IsNullOrWhiteSpace(attribute.Organization))
            throw Missing(componentName, nameof(IntropyComponentAttribute.Organization));
        if (string.IsNullOrWhiteSpace(attribute.Publishes))
            throw Missing(componentName, nameof(IntropyComponentAttribute.Publishes));
        if (attribute.Payload is null)
            throw Missing(componentName, nameof(IntropyComponentAttribute.Payload));
        if (string.IsNullOrWhiteSpace(attribute.EventSource))
            throw Missing(componentName, nameof(IntropyComponentAttribute.EventSource));
        if (!Uri.TryCreate(attribute.EventSource, UriKind.Absolute, out _))
            throw new InvalidOperationException(
                $"{componentName}'s IntropyComponent attribute sets EventSource '{attribute.EventSource}', which is " +
                "not an absolute URI (e.g. urn:Acme:order-gateway): the source is the CloudEvent source of every " +
                "event the component sends.");

        return new ComponentManifest
        {
            ComponentName = attribute.ComponentName,
            Organization = attribute.Organization,
            Publishes = attribute.Publishes,
            Payload = attribute.Payload,
            Port = attribute.Port,
            EndpointPath = attribute.EndpointPath,
            EventSource = attribute.EventSource,
        };
    }

    /// <summary>Returns a member a component kind needs, or fails naming the attribute member.
    /// A member the attribute requires cannot be missing on an attributed class — this guards a
    /// manifest assembled by hand in a host.</summary>
    internal string Require(string? value, string member) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException(
                $"The component manifest of {ComponentName} names no {member}: declare it — with " +
                $"[{nameof(IntropyComponent)}(...)], or on a hand-assembled manifest directly.")
            : value;

    private static InvalidOperationException Missing(string componentName, string member) =>
        new($"{componentName}'s IntropyComponent attribute sets no {member}: the member is required, and the " +
            "framework cannot name the component without it.");
}
