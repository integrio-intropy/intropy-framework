using System.Diagnostics;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>Processes one delivered message and reports how it ended.</summary>
/// <param name="message">The delivered message.</param>
/// <param name="activity">The message's consumer span, for the handler's own tags.</param>
/// <param name="interrupt">Cancelled when the consumer interrupts messages still in flight after
/// its grace period; distinguishes an interruption from a failure.</param>
/// <param name="cancellationToken">Cancels the processing: the interrupt, the per-message time
/// limit, or the sidecar abandoning the call.</param>
internal delegate Task<HandledMessage> MessageHandler(IncomingMessage message, Activity? activity,
    CancellationToken interrupt, CancellationToken cancellationToken);

/// <summary>How a handled message ended, and the route that handled it, if the consumer routes.</summary>
internal readonly record struct HandledMessage(PipelineOutcome Outcome, string? Route = null);

/// <summary>What a <see cref="MessageConsumer"/> consumes and how long it gives each message.</summary>
/// <param name="PubSubName">The pub/sub component its subscription is on.</param>
/// <param name="TopicName">The topic it consumes.</param>
/// <param name="MaxMessageProcessingTime">How long one message may run before it is cancelled and
/// left for redelivery.</param>
/// <param name="ShutdownGracePeriod">How long messages in flight may finish when the consumer
/// stops, before they are interrupted.</param>
/// <param name="AcknowledgeUnrouted">Whether a message no route handles is acknowledged (dropped)
/// instead of left for redelivery, so the broker dead-letters it.</param>
internal sealed record MessageConsumerSettings(
    string PubSubName,
    string TopicName,
    TimeSpan MaxMessageProcessingTime,
    TimeSpan ShutdownGracePeriod,
    bool AcknowledgeUnrouted = false);

/// <summary>
/// Consumes the messages the sidecar pushes to the app callback, for any block that subscribes to a
/// topic: checks the delivery is for its subscription, runs the block's
/// <see cref="MessageHandler"/> in the message's consumer span under the per-message time limit,
/// records the consumed-messages metrics, and answers with the ack. It acknowledges only what was
/// processed (or deliberately dropped) and leaves everything else for redelivery; it never answers
/// <c>DROP</c>, which the sidecar would discard without a Dapr dead-letter topic. Once stopping
/// begins it takes no new message.
/// </summary>
internal sealed class MessageConsumer(
    MessageConsumerSettings settings,
    MessageHandler handler,
    string componentName,
    ILogger logger,
    TimeProvider? time = null,
    ActivityContext run = default) : IDisposable
{
    private readonly InFlightMessages _inFlight = new(time);

    /// <summary>The messages in flight and the idle clock.</summary>
    internal InFlightMessages InFlight => _inFlight;

    internal MessageConsumerSettings Settings => settings;

    /// <summary>Stops taking messages and waits for those in flight, interrupting them after the
    /// grace period. Their acks still reach the sidecar: stop the server only after this.</summary>
    /// <returns>How many messages were still in flight when the grace period ended.</returns>
    internal Task<int> StopAsync() => _inFlight.StopAsync(settings.ShutdownGracePeriod, logger);

    public void Dispose() => _inFlight.Dispose();

    internal async Task<TopicEventResponse> HandleAsync(TopicEventRequest request, CancellationToken callCancellation)
    {
        // The subscription lives in a declarative resource the consumer cannot see: a delivery for
        // another topic means the two disagree. Leave it for redelivery, loudly, rather than lose it.
        if (!string.Equals(request.PubsubName, settings.PubSubName, StringComparison.Ordinal) ||
            !string.Equals(request.Topic, settings.TopicName, StringComparison.Ordinal))
        {
            logger.LogError(
                "{Component} consumes topic {Topic} on {PubSub} but was delivered message {MessageId} from topic {DeliveredTopic} on {DeliveredPubSub}; left for redelivery. Check the component's Subscription resource.",
                componentName, settings.TopicName, settings.PubSubName, request.Id, request.Topic, request.PubsubName);
            HostingMetrics.RecordProcessedMessage(componentName, request.Topic, "failed", "unexpected_subscription",
                TimeSpan.Zero);
            return Retry;
        }

        // Delivered after stop began: leave it for the next consumer.
        if (!_inFlight.TryEnter())
            return Retry;

        try
        {
            return await ProcessAsync(IncomingMessage.From(request), callCancellation);
        }
        finally
        {
            _inFlight.Exit();
        }
    }

    private async Task<TopicEventResponse> ProcessAsync(IncomingMessage message, CancellationToken callCancellation)
    {
        var start = Stopwatch.GetTimestamp();
        using var activity = MessageActivity.StartProcessActivity(message, run);
        activity?.SetTag("cloudevents.event_type", message.Type);

        // The sidecar sets no processing deadline on a pushed message: the consumer enforces its own.
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_inFlight.Interrupt, callCancellation);
        cancellation.CancelAfter(settings.MaxMessageProcessingTime);

        HandledMessage handled;
        try
        {
            handled = await handler(message, activity, _inFlight.Interrupt, cancellation.Token);
        }
        catch (Exception e)
        {
            logger.LogError(e, "Processing message {MessageId} failed; left for redelivery", message.MessageId);
            activity?.AddException(e);
            handled = new HandledMessage(PipelineOutcome.FromException(e, _inFlight.Interrupt, cancellation.Token));
        }

        var outcome = handled.Outcome;
        if (handled.Route is not null)
            activity?.SetTag("intropy.route", handled.Route);
        if (outcome.ErrorType is not null)
        {
            activity?.SetTag("error.type", outcome.ErrorType);
            activity?.SetStatus(ActivityStatusCode.Error, outcome.Description);
        }

        HostingMetrics.RecordProcessedMessage(componentName, settings.TopicName,
            HostingMetrics.OutcomeName(outcome.Outcome), outcome.ErrorType, Stopwatch.GetElapsedTime(start),
            handled.Route);
        return Acknowledges(outcome.Outcome) ? Success : Retry;
    }

    private bool Acknowledges(MessageOutcome outcome) => outcome switch
    {
        MessageOutcome.Processed or MessageOutcome.Skipped => true,
        MessageOutcome.Unrouted => settings.AcknowledgeUnrouted,
        _ => false
    };

    private static TopicEventResponse Success => new() { Status = TopicEventResponse.Types.TopicEventResponseStatus.Success };

    private static TopicEventResponse Retry => new() { Status = TopicEventResponse.Types.TopicEventResponseStatus.Retry };
}
