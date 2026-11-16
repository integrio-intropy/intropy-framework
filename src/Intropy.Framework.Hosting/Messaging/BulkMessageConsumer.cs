using System.Diagnostics;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Status = Dapr.AppCallback.Autogen.Grpc.v1.TopicEventResponse.Types.TopicEventResponseStatus;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// Consumes the batches the sidecar pushes to the bulk app callback: checks the delivery is for its
/// subscription, finds the one route the batch took, runs the whole batch through that route's bulk
/// pipeline under the per-batch time limit, records the consumed-message metrics per entry, logs
/// each message's outcome, and answers with one acknowledgement per entry. Like the single-message
/// <see cref="MessageConsumer"/> it acknowledges only what was processed (or deliberately dropped)
/// and leaves everything else for redelivery; it never answers <c>DROP</c>. Once stopping begins it
/// takes no new batch.
/// </summary>
/// <remarks>
/// One delivery is one route's messages. The sidecar evaluates the subscription's routing rules per
/// message and groups the matches before it delivers, so a batch never mixes CloudEvent types: the
/// consumer routes once for the whole batch and checks the rest of it agrees, rather than routing
/// entry by entry.
/// </remarks>
internal sealed class BulkMessageConsumer : IDisposable
{
    private readonly MessageConsumerSettings _settings;
    private readonly BulkMessageRouteTable _routes;
    private readonly IServiceScopeFactory _scopes;
    private readonly string _componentName;
    private readonly ILogger _logger;
    private readonly ActivityContext _run;
    private readonly InFlightMessages _inFlight;
    private readonly IDisposable _activeMessagesGauge;

    // InFlightMessages counts calls, which is what draining needs; the active-messages gauge reports
    // messages, so a batch counts once per message it carries.
    private int _messagesInFlight;

    internal BulkMessageConsumer(MessageConsumerSettings settings, BulkMessageRouteTable routes,
        IServiceScopeFactory scopes, string componentName, ILogger logger, TimeProvider? time = null,
        ActivityContext run = default)
    {
        _settings = settings;
        _routes = routes;
        _scopes = scopes;
        _componentName = componentName;
        _logger = logger;
        _run = run;
        _inFlight = new InFlightMessages(time);
        _activeMessagesGauge = HostingMetrics.TrackActiveMessages(componentName, settings.PubSubName,
            settings.TopicName, () => Volatile.Read(ref _messagesInFlight));
    }

    /// <summary>The batches in flight and the idle clock.</summary>
    internal InFlightMessages InFlight => _inFlight;

    internal MessageConsumerSettings Settings => _settings;

    /// <summary>Stops taking batches and waits for those in flight, interrupting them after the
    /// grace period. Their acks still reach the sidecar: stop the server only after this.</summary>
    /// <returns>How many batches were still in flight when the grace period ended.</returns>
    internal Task<int> StopAsync() => _inFlight.StopAsync(_settings.ShutdownGracePeriod, _logger);

    public void Dispose()
    {
        _activeMessagesGauge.Dispose();
        _inFlight.Dispose();
    }

    /// <summary>Handles one bulk delivery.</summary>
    /// <param name="request">The delivery.</param>
    /// <param name="callCancellation">Cancelled when the sidecar abandons the call.</param>
    internal async Task<TopicEventBulkResponse> HandleAsync(TopicEventBulkRequest request,
        CancellationToken callCancellation)
    {
        if (request.Entries.Count == 0)
            return new TopicEventBulkResponse();

        // The subscription lives in a declarative resource the consumer cannot see: when the
        // consumer names its topic too, a delivery for another topic means the two disagree. Leave
        // it for redelivery, loudly, rather than lose it.
        if (!Expects(_settings.PubSubName, request.PubsubName) || !Expects(_settings.TopicName, request.Topic))
        {
            RejectUnexpectedSubscription(request);
            return RespondAll(request, Status.Retry);
        }

        // Delivered after stop began: leave it for the next consumer.
        if (!_inFlight.TryEnter())
        {
            _logger.LogDebug("Batch {BatchId} of {Count} messages was delivered after stopping began; left for redelivery",
                request.Id, request.Entries.Count);
            RecordEach(request, HostingMetrics.RejectedOutcome, errorType: null);
            return RespondAll(request, Status.Retry);
        }

        Interlocked.Add(ref _messagesInFlight, request.Entries.Count);
        try
        {
            return await ProcessAsync(request, callCancellation);
        }
        finally
        {
            Interlocked.Add(ref _messagesInFlight, -request.Entries.Count);
            _inFlight.Exit();
        }
    }

    private static bool Expects(string expected, string delivered) =>
        expected.Length == 0 || string.Equals(delivered, expected, StringComparison.Ordinal);

    private async Task<TopicEventBulkResponse> ProcessAsync(TopicEventBulkRequest request,
        CancellationToken callCancellation)
    {
        var start = Stopwatch.GetTimestamp();
        var statuses = new Dictionary<string, Status>(StringComparer.Ordinal);

        // An entry the sidecar delivered as raw bytes carries no CloudEvent type to route on. It is
        // the one shape this consumer cannot place; it is reported on its own rather than dragging
        // the rest of the batch down with it.
        var messages = new List<IncomingMessage>(request.Entries.Count);
        var entryIds = new List<string>(request.Entries.Count);
        foreach (var entry in request.Entries)
        {
            if (IncomingMessage.FromBulkEntry(entry, request) is { } message)
            {
                messages.Add(message);
                entryIds.Add(entry.EntryId);
            }
            else
            {
                RejectUndecodable(request, entry.EntryId);
                statuses[entry.EntryId] = UnroutedStatus;
            }
        }

        if (messages.Count == 0)
            return Respond(request, statuses);

        // Each message is processed in its own trace, under its own span, as a message delivered on
        // its own is. Starting a span makes it current: clear it so the batch's work does not run
        // under the last message's span.
        var spans = new Activity?[messages.Count];
        for (var i = 0; i < messages.Count; i++)
            spans[i] = MessageActivity.StartProcessActivity(messages[i], _componentName, _run);
        Activity.Current = null;
        try
        {
            // The sidecar sets no processing deadline on a pushed batch: the consumer enforces its own.
            using var timeout = new CancellationTokenSource();
            timeout.CancelAfter(_settings.MaxMessageProcessingTime);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_inFlight.Interrupt,
                callCancellation, timeout.Token);

            var route = Route(messages[0]);
            var (outcomes, ran) = await OutcomesAsync(request, route, messages, spans, cancellation.Token);

            for (var i = 0; i < messages.Count; i++)
            {
                var outcome = outcomes[i].WithCancellationCause(_settings.MaxMessageProcessingTime, timeout.Token,
                    callCancellation);
                var acknowledged = Acknowledges(outcome.Outcome);

                // A message left for redelivery is an error; one deliberately dropped is not.
                var errorType = acknowledged ? null : outcome.ErrorType;
                statuses[entryIds[i]] = acknowledged ? Status.Success : Status.Retry;
                MessageActivity.Complete(spans[i], outcome, route?.Name, errorType);
                if (ran)
                    Log(messages[i], route?.Name, outcome);

                // A batch is processed as one unit, so its messages have no duration of their own:
                // each is counted with its outcome, and the batch's time is recorded once.
                Record(messages[i], HostingMetrics.OutcomeName(outcome.Outcome), errorType, duration: null,
                    route?.Name);
            }

            if (ran)
                HostingMetrics.RecordBatchProcessDuration(_componentName, request.PubsubName, request.Topic,
                    route!.Name, Stopwatch.GetElapsedTime(start));
            return Respond(request, statuses);
        }
        finally
        {
            foreach (var span in spans)
                span?.Dispose();
        }
    }

    private const string HeterogeneousBatch = "heterogeneous_batch";
    private const string OutcomeCountMismatch = "outcome_count_mismatch";

    /// <summary>Each message's outcome, in order, and whether the route ran the batch: its verdicts
    /// when it did, otherwise the reason it did not, the same for every message.</summary>
    private async Task<(IReadOnlyList<PipelineOutcome> Outcomes, bool Ran)> OutcomesAsync(
        TopicEventBulkRequest request, IBulkMessageRoute? route, List<IncomingMessage> messages, Activity?[] spans,
        CancellationToken cancellation)
    {
        if (route is null)
        {
            LogUnrouted(messages);
            return (Each(messages, new PipelineOutcome(MessageOutcome.Unrouted, PipelineOutcome.Unrouted,
                "No route handles the batch's event type")), false);
        }

        if (messages.Any(message => !route.Matches(message)))
        {
            // Dapr groups a batch by the route its rules selected, so this cannot happen against a
            // sidecar honouring its own contract. Fail loudly and leave the batch for redelivery
            // rather than run messages through a pipeline not written for them.
            _logger.LogError(
                "Batch {BatchId} routed to {Route} carries more than one CloudEvent type ({Types}); left for redelivery. A bulk delivery is expected to carry one route's messages.",
                request.Id, route.Name, string.Join(", ", messages.Select(m => m.Type ?? "(none)").Distinct()));
            return (Each(messages, PipelineOutcome.Failed(HeterogeneousBatch,
                "The batch carries more than one CloudEvent type")), false);
        }

        var outcomes = await ExecuteAsync(route, messages, spans, cancellation);
        if (outcomes.Count == messages.Count)
            return (outcomes, true);

        _logger.LogError(
            "Route {Route} answered {Answered} outcomes for {Sent} messages of batch {BatchId}; left for redelivery.",
            route.Name, outcomes.Count, messages.Count, request.Id);
        return (Each(messages, PipelineOutcome.Failed(OutcomeCountMismatch,
            "The route answered the wrong number of outcomes")), false);
    }

    private static PipelineOutcome[] Each(List<IncomingMessage> messages, PipelineOutcome outcome) =>
        [.. Enumerable.Repeat(outcome, messages.Count)];

    /// <summary>The route for the batch, or null when none handles it: no matching route, or the
    /// subscription's rules selected none and delivered it on their default route.</summary>
    private IBulkMessageRoute? Route(IncomingMessage first) =>
        _routes.IsRouting && first.IsUnhandled ? null : _routes.Find(first);

    private async Task<IReadOnlyList<PipelineOutcome>> ExecuteAsync(IBulkMessageRoute route,
        List<IncomingMessage> messages, Activity?[] spans, CancellationToken cancellationToken)
    {
        try
        {
            return await route.ExecuteBatchAsync(messages, spans, _scopes, _componentName, _inFlight.Interrupt,
                cancellationToken);
        }
        catch (Exception e)
        {
            var failure = PipelineOutcome.FromException(e, _inFlight.Interrupt, cancellationToken);
            return [.. Enumerable.Repeat(failure, messages.Count)];
        }
    }

    private void RejectUnexpectedSubscription(TopicEventBulkRequest request)
    {
        const string errorType = "unexpected_subscription";
        _logger.LogError(
            "{Component} consumes topic {Topic} on {PubSub} but was delivered batch {BatchId} from topic {DeliveredTopic} on {DeliveredPubSub}; left for redelivery. Check the component's Subscription resource.",
            _componentName, _settings.TopicName, _settings.PubSubName, request.Id, request.Topic, request.PubsubName);
        RecordEach(request, "failed", errorType);
    }

    private void RejectUndecodable(TopicEventBulkRequest request, string entryId)
    {
        _logger.LogWarning(
            "Entry {EntryId} of batch {BatchId} was delivered as raw bytes rather than a CloudEvent, so it carries no event type to route on; {Handling}",
            entryId, request.Id, _settings.AcknowledgeUnrouted ? "dropped" : "left for redelivery");
        HostingMetrics.RecordConsumedMessage(_componentName, request.PubsubName, request.Topic,
            HostingMetrics.OutcomeName(MessageOutcome.Unrouted), errorType: null, duration: null);
    }

    private void LogUnrouted(List<IncomingMessage> messages) =>
        _logger.LogWarning(
            "A batch of {Count} messages of event type {EventType} is unrouted: no route handles it; {Handling}",
            messages.Count, messages[0].Type, _settings.AcknowledgeUnrouted ? "dropped" : "left for redelivery");

    private void Log(IncomingMessage message, string? route, PipelineOutcome outcome)
    {
        switch (outcome.Outcome)
        {
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
            default:
                _logger.LogWarning(outcome.Exception,
                    "Processing message {MessageId} failed ({ErrorType}: {Description}); left for redelivery",
                    message.MessageId, outcome.ErrorType, outcome.Description);
                break;
        }
    }

    private void Record(IncomingMessage message, string outcome, string? errorType, TimeSpan? duration,
        string? route = null) =>
        HostingMetrics.RecordConsumedMessage(_componentName, message.PubSubName, message.TopicName, outcome, errorType,
            duration, route);

    private void RecordEach(TopicEventBulkRequest request, string outcome, string? errorType)
    {
        for (var i = 0; i < request.Entries.Count; i++)
            HostingMetrics.RecordConsumedMessage(_componentName, request.PubsubName, request.Topic, outcome, errorType,
                duration: null);
    }

    private bool Acknowledges(MessageOutcome outcome) => outcome switch
    {
        MessageOutcome.Processed or MessageOutcome.Skipped or MessageOutcome.Probe => true,
        MessageOutcome.Unrouted => _settings.AcknowledgeUnrouted,
        _ => false
    };

    private Status UnroutedStatus => _settings.AcknowledgeUnrouted ? Status.Success : Status.Retry;

    /// <summary>One status per delivered entry, in delivery order; anything the batch did not
    /// resolve is left for redelivery.</summary>
    private static TopicEventBulkResponse Respond(TopicEventBulkRequest request, Dictionary<string, Status> statuses)
    {
        var response = new TopicEventBulkResponse();
        foreach (var entry in request.Entries)
            response.Statuses.Add(new TopicEventBulkResponseEntry
            {
                EntryId = entry.EntryId,
                Status = statuses.TryGetValue(entry.EntryId, out var status) ? status : Status.Retry
            });
        return response;
    }

    private static TopicEventBulkResponse RespondAll(TopicEventBulkRequest request, Status status)
    {
        var response = new TopicEventBulkResponse();
        foreach (var entry in request.Entries)
            response.Statuses.Add(new TopicEventBulkResponseEntry { EntryId = entry.EntryId, Status = status });
        return response;
    }
}
