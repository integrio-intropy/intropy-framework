using Dapr.Client;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Components;

/// <summary>
/// What every component that publishes shares, whatever brings its items in: the one publisher of
/// its one message, typed and sourced from the manifest.
/// </summary>
internal static class ComponentPublishing
{
    /// <summary>Registers the publisher. From the manifest: the event type (<c>publishes</c>) and the
    /// event source (<c>eventSourceValue</c>). From the deployment: the pub/sub and the topic
    /// (<c>INTROPY_PUBSUB_NAME</c>, <c>INTROPY_TOPIC_NAME</c>), resolved when a pipeline is first
    /// composed, at startup.</summary>
    /// <exception cref="InvalidOperationException">The manifest lacks a value, or the event source
    /// is not an absolute URI.</exception>
    internal static void AddPublisher(IServiceCollection services, ComponentManifest manifest)
    {
        var eventType = manifest.Require(manifest.Publishes, "publishes");
        var sourceValue = manifest.Require(manifest.EventSource, "event source");
        if (!Uri.TryCreate(sourceValue, UriKind.Absolute, out var eventSource))
            throw new InvalidOperationException(
                $"{manifest.ComponentName}'s IntropyComponent attribute sets EventSource '{sourceValue}', which is " +
                "not an absolute URI.");

        // The publisher is a service like the other externals, so a test swaps the
        // SendStep<Context> registration for a fake exactly as it swaps the clients.
        services.AddSingleton<SendStep<Context>>(provider =>
        {
            var options = provider.GetRequiredService<FrameworkOptions>();
            return new DaprTopicPublisher<Context>(
                provider.GetRequiredService<DaprClient>(),
                options.PubSubName ?? throw new InvalidOperationException(
                    "INTROPY_PUBSUB_NAME is not set, so the system's pub/sub is unknown: run the " +
                    "component under its system host, or set FrameworkOptions.PubSubName."),
                options.TopicName ?? throw new InvalidOperationException(
                    "INTROPY_TOPIC_NAME is not set, so the topic is unknown: run the component " +
                    "under its system host, or set FrameworkOptions.TopicName."),
                eventSource,
                eventType);
        });
    }

    /// <summary>Checks that a route publishes the message's projection: one component, one
    /// message. The payload is typed — the attribute's <see cref="IntropyComponentAttribute.Payload"/>
    /// against the route's <c>TOutput</c> — so the check compares types, not names.</summary>
    /// <exception cref="InvalidOperationException">The route's payload type is not the component's
    /// message's projection.</exception>
    internal static void EnsurePayload(Type output, ComponentManifest manifest, string route)
    {
        if (manifest.Payload is { } payload && payload != output)
            throw new InvalidOperationException(
                $"{route} publishes {output.Name}, but {manifest.ComponentName}'s message '{manifest.Publishes}' " +
                $"projects to {payload.Name}: the payload type is the message's projection, so the two must agree.");
    }
}
