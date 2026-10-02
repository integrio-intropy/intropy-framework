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
/// records the consumed-messages metrics, logs the outcome, and answers with the ack. It
/// acknowledges only what was processed (or deliberately dropped) and leaves everything else for
/// redelivery; it never answers <c>DROP</c>, which the sidecar would discard without a Dapr
/// dead-letter topic. Once stopping begins it takes no new message.
/// </summary>
internal sealed class MessageConsumer : IDisposable
{
    private readonly MessageConsumerSettings _settings;
    private readonly MessageHandler _handler;
    private readonly string _componentName;
    private readonly ILogger _logger;
    private readonly ActivityContext _run;
    private readonly InFlightMessages _inFlight;
    private readonly IDisposable _activeMessages;

    internal MessageConsumer(MessageConsumerSettings settings, MessageHandler handler, string componentName,
        ILogger logger, TimeProvider? time = null, ActivityContext run = default)
    {
        _settings = settings;
        _handler = handler;
        _componentName = componentName;
        _logger = logger;
        _run = run;
        _inFlight = new InFlightMessages(time);
        _activeMessages = HostingMetrics.TrackActiveMessages(componentName, settings.PubSubName, settings.TopicName,
            () => _inFlight.Count);
    }

    /// <summary>The messages in flight and the idle clock.</summary>
    internal InFlightMessages InFlight => _inFlight;

    internal MessageConsumerSettings Settings => _settings;

    /// <summary>Stops taking messages and waits for those in flight, interrupting them after the
    /// grace period. Their acks still reach the sidecar: stop the server only after this.</summary>
    /// <returns>How many messages were still in flight when the grace period ended.</returns>
    internal Task<int> StopAsync() => _inFlight.StopAsync(_settings.ShutdownGracePeriod, _logger);

    public void Dispose()
    {
        _activeMessages.Dispose();
        _inFlight.Dispose();
    }

    /// <summary>Handles one delivery.</summary>
    /// <param name="request">The delivery.</param>
    /// <param name="callCancellation">Cancelled when the sidecar abandons the call.</param>
    /// <param name="delivery">The trace context the sidecar delivered it with, linked from the
    /// message's span; <see langword="default"/> for none.</param>
    internal async Task<TopicEventResponse> HandleAsync(TopicEventRequest request, CancellationToken callCancellation,
        ActivityContext delivery = default)
    {
        // The subscription lives in a declarative resource the consumer cannot see: a delivery for
        // another topic means the two disagree. Leave it for redelivery, loudly, rather than lose it.
        if (!string.Equals(request.PubsubName, _settings.PubSubName, StringComparison.Ordinal) ||
            !string.Equals(request.Topic, _settings.TopicName, StringComparison.Ordinal))
        {
            RejectUnexpectedSubscription(IncomingMessage.From(request), delivery);
            return Retry;
        }

        // Delivered after stop began: leave it for the next consumer.
        if (!_inFlight.TryEnter())
        {
            _logger.LogDebug("Message {MessageId} was delivered after stopping began; left for redelivery", request.Id);
            HostingMetrics.RecordConsumedMessage(_componentName, request.PubsubName, request.Topic,
                HostingMetrics.RejectedOutcome, errorType: null, duration: null);
            return Retry;
        }

        try
        {
            return await ProcessAsync(IncomingMessage.From(request), delivery, callCancellation);
        }
        finally
        {
            _inFlight.Exit();
        }
    }

    private void RejectUnexpectedSubscription(IncomingMessage message, ActivityContext delivery)
    {
        const string errorType = "unexpected_subscription";
        using var activity = MessageActivity.StartProcessActivity(message, _componentName, _run, delivery);
        activity?.SetTag(MessagingAttributes.ErrorType, errorType);
        activity?.SetStatus(ActivityStatusCode.Error,
            $"Consumes topic '{_settings.TopicName}' on '{_settings.PubSubName}'; delivered from another subscription");
        _logger.LogError(
            "{Component} consumes topic {Topic} on {PubSub} but was delivered message {MessageId} from topic {DeliveredTopic} on {DeliveredPubSub}; left for redelivery. Check the component's Subscription resource.",
            _componentName, _settings.TopicName, _settings.PubSubName, message.MessageId, message.TopicName,
            message.PubSubName);
        HostingMetrics.RecordConsumedMessage(_componentName, message.PubSubName, message.TopicName, "failed", errorType,
            duration: null);
    }

    private async Task<TopicEventResponse> ProcessAsync(IncomingMessage message, ActivityContext delivery,
        CancellationToken callCancellation)
    {
        var start = Stopwatch.GetTimestamp();
        using var activity = MessageActivity.StartProcessActivity(message, _componentName, _run, delivery);
        using var scope = _logger.BeginScope(LogScope(message));

        // The sidecar sets no processing deadline on a pushed message: the consumer enforces its own.
        using var timeout = new CancellationTokenSource();
        timeout.CancelAfter(_settings.MaxMessageProcessingTime);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_inFlight.Interrupt,
            callCancellation, timeout.Token);

        HandledMessage handled;
        try
        {
            handled = await _handler(message, activity, _inFlight.Interrupt, cancellation.Token);
        }
        catch (Exception e)
        {
            handled = new HandledMessage(PipelineOutcome.FromException(e, _inFlight.Interrupt, cancellation.Token));
        }

        var outcome = handled.Outcome.WithCancellationCause(_settings.MaxMessageProcessingTime, timeout.Token,
            callCancellation);
        var acknowledged = Acknowledges(outcome.Outcome);

        // A message left for redelivery is an error; one deliberately dropped is not.
        var errorType = acknowledged ? null : outcome.ErrorType;
        if (activity is not null)
        {
            activity.SetTag(MessagingAttributes.MessageOutcome, HostingMetrics.OutcomeName(outcome.Outcome));
            if (handled.Route is not null)
                activity.SetTag(MessagingAttributes.LoaderRoute, handled.Route);
            if (outcome.Exception is not null)
                activity.AddException(outcome.Exception);
            if (errorType is not null)
            {
                activity.SetTag(MessagingAttributes.ErrorType, errorType);
                activity.SetStatus(ActivityStatusCode.Error, outcome.Description);
            }
        }

        Log(message, handled.Route, outcome);
        HostingMetrics.RecordConsumedMessage(_componentName, message.PubSubName, message.TopicName,
            HostingMetrics.OutcomeName(outcome.Outcome), errorType, Stopwatch.GetElapsedTime(start), handled.Route);
        return acknowledged ? Success : Retry;
    }

    private static Dictionary<string, object?> LogScope(IncomingMessage message) => new()
    {
        [MessagingAttributes.DestinationName] = message.TopicName,
        [MessagingAttributes.MessageId] = message.MessageId,
        [MessagingAttributes.EventType] = message.Type
    };

    private void Log(IncomingMessage message, string? route, PipelineOutcome outcome)
    {
        switch (outcome.Outcome)
        {
            case MessageOutcome.Processed when route is null:
                _logger.LogDebug("Processed message {MessageId}", message.MessageId);
                break;
            case MessageOutcome.Processed:
                _logger.LogDebug("Processed message {MessageId} on route {Route}", message.MessageId, route);
                break;
            case MessageOutcome.Skipped:
                _logger.LogDebug("Message {MessageId} is a duplicate; consumed", message.MessageId);
                break;
            case MessageOutcome.Interrupted:
                _logger.LogInformation("Message {MessageId} was interrupted by the host stopping; left for redelivery",
                    message.MessageId);
                break;
            case MessageOutcome.Unrouted:
                _logger.LogWarning("Message {MessageId} of event type {EventType} is unrouted: {Description}; {Handling}",
                    message.MessageId, message.Type, outcome.Description,
                    _settings.AcknowledgeUnrouted ? "dropped" : "left for redelivery");
                break;
            default:
                _logger.LogWarning(outcome.Exception,
                    "Processing message {MessageId} failed ({ErrorType}: {Description}); left for redelivery",
                    message.MessageId, outcome.ErrorType, outcome.Description);
                break;
        }
    }

    private bool Acknowledges(MessageOutcome outcome) => outcome switch
    {
        MessageOutcome.Processed or MessageOutcome.Skipped => true,
        MessageOutcome.Unrouted => _settings.AcknowledgeUnrouted,
        _ => false
    };

    private static TopicEventResponse Success => new() { Status = TopicEventResponse.Types.TopicEventResponseStatus.Success };

    private static TopicEventResponse Retry => new() { Status = TopicEventResponse.Types.TopicEventResponseStatus.Retry };
}
