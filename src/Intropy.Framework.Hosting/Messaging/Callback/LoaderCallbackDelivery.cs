using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Hosting.Messaging.Streaming;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging.Callback;

/// <summary>
/// Handles the messages the sidecar pushes to a loader's app callback, one per call: reads the
/// delivery, runs it through the loader under <see cref="LoaderOptions.MaxMessageProcessingTime"/>,
/// and answers with its ack.
/// </summary>
internal sealed class LoaderCallbackDelivery(
    LoaderMessageHandler handler,
    LoaderOptions options,
    string componentName,
    ILogger<LoaderCallbackDelivery> logger) : IDisposable
{
    private const string RetryCountKey = "retrycount";
    private static readonly IReadOnlyDictionary<string, Value> s_noExtensions = new Dictionary<string, Value>();

    private readonly CancellationTokenSource _interrupt = new();

    public void Dispose() => _interrupt.Dispose();

    internal async Task<TopicEventResponse> HandleAsync(TopicEventRequest request, CancellationToken callCancellation)
    {
        // The subscription lives in a declarative resource the loader cannot see: a delivery for
        // another topic means the two disagree. Leave it for redelivery, loudly, rather than lose it.
        if (!string.Equals(request.PubsubName, options.PubSubName, StringComparison.Ordinal) ||
            !string.Equals(request.Topic, options.TopicName, StringComparison.Ordinal))
        {
            logger.LogError(
                "Loader {Component} consumes topic {Topic} on {PubSub} but was delivered message {MessageId} from topic {DeliveredTopic} on {DeliveredPubSub}; left for redelivery. Check the loader's Subscription resource.",
                componentName, options.TopicName, options.PubSubName, request.Id, request.Topic, request.PubsubName);
            HostingMetrics.RecordProcessedMessage(componentName, request.Topic, "failed", "unexpected_subscription",
                TimeSpan.Zero);
            return Response(TopicEventResponse.Types.TopicEventResponseStatus.Retry);
        }

        IReadOnlyDictionary<string, Value> extensions = request.Extensions?.Fields ?? s_noExtensions;
        var message = TopicMessageReader.Read(request.Id, request.Source, request.Type, request.DataContentType,
            request.Data.Memory, extensions, extensions.ContainsKey(RetryCountKey));

        // The sidecar sets no processing deadline on a pushed message: the loader enforces its own.
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_interrupt.Token, callCancellation);
        cancellation.CancelAfter(options.MaxMessageProcessingTime);

        var outcome = await handler.ProcessAsync(message, extensions, options.TopicName, _interrupt.Token,
            cancellation.Token);
        return Response(LoaderAcks.ToStatus(outcome.Outcome, options.Unrouted));
    }

    private static TopicEventResponse Response(TopicEventResponse.Types.TopicEventResponseStatus status) =>
        new() { Status = status };
}
