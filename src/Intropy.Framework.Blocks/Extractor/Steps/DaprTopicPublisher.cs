using CloudNative.CloudEvents;
using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// A step that publishes CloudEvents to a Dapr pub/sub topic.
/// This step sets the source and type on the CloudEvent from the configured values,
/// then serializes the event and publishes it to the specified topic.
/// </summary>
/// <typeparam name="TCtx">The type of the context.</typeparam>
/// <param name="daprClient">The Dapr client used for pub/sub publishing.</param>
/// <param name="pubSubName">The name of the Dapr pub/sub component.</param>
/// <param name="topicName">The topic to publish to.</param>
/// <param name="source">The CloudEvent source URI identifying where the data came from.</param>
/// <param name="type">The CloudEvent type identifying the kind of event.</param>
public class DaprTopicPublisher<TCtx>(
    DaprClient daprClient,
    string pubSubName,
    string topicName,
    Uri source,
    string type) : SendStep<TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override async Task<(TechnicalStepResult<CloudEvent> Result, TCtx Context)> ExecuteAsync(CloudEvent input,
        TCtx context, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(input);

        // Validate required properties that should be set by the SerializeStep
        if (string.IsNullOrEmpty(input.Subject))
            throw new InvalidOperationException(
                "CloudEvent.Subject must be set by the SerializeStep. The subject identifies the entity this event is about.");

        if (input.Time is null)
            throw new InvalidOperationException(
                "CloudEvent.Time must be set by the SerializeStep. The time indicates when the event occurred in the source system.");

        // Set source and type from configuration
        input.Source = source;
        input.Type = type;

        // Serialize CloudEvent to JSON bytes
        var bytes = CloudEventSerializer.Formatter.EncodeStructuredModeMessage(input, out _);

        // Publish to Dapr pub/sub topic
        await daprClient.PublishByteEventAsync(pubSubName, topicName, bytes, "application/cloudevents+json",
            cancellationToken: ct);

        return (new TechnicalStepResult<CloudEvent>.Success(input), context);
    }
}
