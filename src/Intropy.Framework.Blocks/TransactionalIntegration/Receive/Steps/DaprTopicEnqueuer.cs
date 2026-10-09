using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;

/// <summary>
/// Publishes each source item to a Dapr pub/sub topic as a structured CloudEvent
/// (<c>application/cloudevents+json</c>). The envelope carries the context metadata and the
/// current W3C trace parent, so the send side restores both. Probes the topic
/// (<see cref="IInternalQueueProbe"/>) the same way.
/// </summary>
/// <typeparam name="TCtx">The type of the context.</typeparam>
/// <param name="daprClient">The Dapr client used for pub/sub publishing.</param>
/// <param name="pubSubName">The name of the Dapr pub/sub component.</param>
/// <param name="topicName">The topic to publish to.</param>
/// <param name="options">The framework options, for the envelope's source.</param>
public class DaprTopicEnqueuer<TCtx>(
    DaprClient daprClient,
    string pubSubName,
    string topicName,
    FrameworkOptions options) : EnqueueStep<TCtx>(options), IInternalQueueProbe where TCtx : Context
{
    /// <inheritdoc/>
    protected override string DestinationName => topicName;

    /// <inheritdoc/>
    protected override string MessagingSystem => "dapr";

    /// <inheritdoc/>
    public override async Task<(TechnicalStepResult<SourceItem> Result, TCtx Context)> ExecuteAsync(SourceItem input,
        ReadOnlyMemory<byte> cloudEvent, TCtx context, CancellationToken ct)
    {
        await daprClient.PublishByteEventAsync(pubSubName, topicName, cloudEvent, "application/cloudevents+json",
            cancellationToken: ct);
        return (new TechnicalStepResult<SourceItem>.Success(input), context);
    }

    /// <inheritdoc/>
    public Task PublishProbeAsync(string probeId, CancellationToken ct) =>
        daprClient.PublishByteEventAsync(pubSubName, topicName, EncodeProbe(probeId), "application/cloudevents+json",
            cancellationToken: ct);
}
