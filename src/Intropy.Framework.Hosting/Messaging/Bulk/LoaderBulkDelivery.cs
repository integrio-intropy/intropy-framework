using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Hosting.Messaging.Streaming;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging.Bulk;

/// <summary>
/// A batch loader's side of Dapr's gRPC app callback: announces the loader's bulk subscription and
/// handles each delivered batch — one consumer span for the batch, linked to every entry's producer
/// trace — mapping every entry's outcome to its status. Owned by the loader's callback services.
/// </summary>
internal sealed class LoaderBulkDelivery(
    LoaderMessageHandler handler,
    LoaderOptions options,
    string componentName,
    ILogger<LoaderBulkDelivery> logger) : IDisposable
{
    private readonly CancellationTokenSource _interrupt = new();

    /// <summary>Interrupts the batches still running, when the host's shutdown grace period ends:
    /// their entries are left for redelivery.</summary>
    internal void Interrupt() => _interrupt.Cancel();

    public void Dispose() => _interrupt.Dispose();

    internal ListTopicSubscriptionsResponse Subscriptions()
    {
        var subscription = new TopicSubscription
        {
            PubsubName = options.PubSubName,
            Topic = options.TopicName,
            BulkSubscribe = new BulkSubscribeConfig
            {
                Enabled = true,
                MaxMessagesCount = options.MaxBatchSize,
                MaxAwaitDurationMs = (int)options.MaxBatchWait.TotalMilliseconds
            }
        };
        if (!string.IsNullOrWhiteSpace(options.DeadLetterTopic))
            subscription.DeadLetterTopic = options.DeadLetterTopic;

        var response = new ListTopicSubscriptionsResponse();
        response.Subscriptions.Add(subscription);
        return response;
    }

    internal async Task<TopicEventBulkResponse> HandleBulkAsync(TopicEventBulkRequest request,
        CancellationToken cancellationToken)
    {
        var entries = request.Entries.Select(Read).ToList();
        var statuses = await HandleAsync(entries, cancellationToken);

        var response = new TopicEventBulkResponse();
        for (var i = 0; i < entries.Count; i++)
            response.Statuses.Add(new TopicEventBulkResponseEntry { EntryId = request.Entries[i].EntryId, Status = statuses[i] });
        return response;
    }

    internal async Task<TopicEventResponse> HandleSingleAsync(TopicEventRequest request,
        CancellationToken cancellationToken)
    {
        // Dapr delivers one message at a time when it falls back from bulk (or before the
        // subscription's bulk settings apply): it is a batch of one.
        var entry = TopicMessageReader.Read(request.Id, request.Source, request.Type, request.DataContentType,
            request.Data.Memory, Fields(request.Extensions), isRetry: false);
        var statuses = await HandleAsync([(entry, Fields(request.Extensions))], cancellationToken);
        return new TopicEventResponse { Status = statuses[0] };
    }

    private async Task<IReadOnlyList<TopicEventResponse.Types.TopicEventResponseStatus>> HandleAsync(
        IReadOnlyList<(IncomingMessage Message, IReadOnlyDictionary<string, Value> Extensions)> entries,
        CancellationToken cancellationToken)
    {
        var start = Stopwatch.GetTimestamp();
        using var activity = StartBatchActivity(entries);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_interrupt.Token, cancellationToken);
        cancellation.CancelAfter(options.MaxMessageProcessingTime);

        var results = await handler.HandleBatchAsync([.. entries.Select(e => e.Message)], _interrupt.Token,
            cancellation.Token);

        var elapsed = Stopwatch.GetElapsedTime(start);
        var statuses = new List<TopicEventResponse.Types.TopicEventResponseStatus>(results.Count);
        foreach (var (route, outcome) in results)
        {
            HostingMetrics.RecordProcessedMessage(componentName, options.TopicName,
                HostingMetrics.OutcomeName(outcome.Outcome), outcome.ErrorType, elapsed, route?.Name);
            statuses.Add(LoaderAcks.ToResponse(outcome.Outcome, options.Unrouted) switch
            {
                Dapr.Messaging.PublishSubscribe.TopicResponseAction.Success => TopicEventResponse.Types.TopicEventResponseStatus.Success,
                Dapr.Messaging.PublishSubscribe.TopicResponseAction.Drop => TopicEventResponse.Types.TopicEventResponseStatus.Drop,
                _ => TopicEventResponse.Types.TopicEventResponseStatus.Retry
            });
        }

        var redelivered = statuses.Count(s => s == TopicEventResponse.Types.TopicEventResponseStatus.Retry);
        var dropped = statuses.Count(s => s == TopicEventResponse.Types.TopicEventResponseStatus.Drop);
        activity?.SetTag("intropy.batch.redelivered_entries", redelivered);
        activity?.SetTag("intropy.batch.dropped_entries", dropped);
        if (redelivered + dropped > 0)
            activity?.SetStatus(ActivityStatusCode.Error,
                $"{redelivered} of {results.Count} entries left for redelivery, {dropped} dropped");
        logger.LogInformation(
            "Handled a batch of {Count} message(s) from topic {Topic}: {Redelivered} left for redelivery, {Dropped} dropped",
            results.Count, options.TopicName, redelivered, dropped);
        return statuses;
    }

    /// <summary>The batch's consumer span, following the OpenTelemetry messaging conventions for
    /// batches: a new trace, linked to the producer trace of every entry that carries one.</summary>
    private Activity? StartBatchActivity(
        IReadOnlyList<(IncomingMessage Message, IReadOnlyDictionary<string, Value> Extensions)> entries)
    {
        var links = new List<ActivityLink>();
        foreach (var (_, extensions) in entries)
        {
            var traceParent = TopicMessageReader.GetString(extensions, "traceparent");
            if (!string.IsNullOrEmpty(traceParent) && ActivityContext.TryParse(traceParent,
                    TopicMessageReader.GetString(extensions, "tracestate"), isRemote: true, out var producer))
                links.Add(new ActivityLink(producer));
        }

        Activity.Current = null;
        var activity = ActivitySourceProvider.ActivitySource.StartActivity($"process {options.TopicName}",
            ActivityKind.Consumer, default(ActivityContext), links: links);
        activity?.SetTag("messaging.system", "dapr");
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("messaging.operation.name", "process");
        activity?.SetTag("messaging.destination.name", options.TopicName);
        activity?.SetTag("messaging.batch.message_count", entries.Count);
        return activity;
    }

    private static (IncomingMessage, IReadOnlyDictionary<string, Value>) Read(TopicEventBulkRequestEntry entry)
    {
        if (entry.EventCase == TopicEventBulkRequestEntry.EventOneofCase.CloudEvent)
        {
            var cloudEvent = entry.CloudEvent;
            var extensions = Fields(cloudEvent.Extensions);
            return (TopicMessageReader.Read(entry.EntryId, cloudEvent.Source, cloudEvent.Type,
                cloudEvent.DataContentType, cloudEvent.Data.Memory, extensions, isRetry: false), extensions);
        }

        return ReadStructured(entry.EntryId, entry.Bytes.Memory);
    }

    /// <summary>Reads an entry delivered as raw bytes: a structured-mode CloudEvents JSON envelope
    /// when it parses as one, otherwise a payload without CloudEvent attributes (which only a loader
    /// without routes can handle).</summary>
    private static (IncomingMessage, IReadOnlyDictionary<string, Value>) ReadStructured(string entryId,
        ReadOnlyMemory<byte> bytes)
    {
        try
        {
            using var envelope = JsonDocument.Parse(bytes);
            var root = envelope.RootElement;
            var extensions = new Dictionary<string, Value>(StringComparer.Ordinal);
            foreach (var property in root.EnumerateObject())
                if (property.Name is not ("data" or "data_base64") && property.Value.ValueKind == JsonValueKind.String)
                    extensions[property.Name] = Value.ForString(property.Value.GetString());

            var data = root.TryGetProperty("data", out var value) ? Encoding.UTF8.GetBytes(value.GetRawText()) : [];
            return (TopicMessageReader.Read(String(root, "id") ?? entryId, String(root, "source"), String(root, "type"),
                String(root, "datacontenttype"), data, extensions, isRetry: false), extensions);
        }
        catch (JsonException)
        {
            var none = new Dictionary<string, Value>();
            return (TopicMessageReader.Read(entryId, null, null, null, bytes, none, isRetry: false), none);
        }

        static string? String(JsonElement root, string name) =>
            root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;
    }

    private static IReadOnlyDictionary<string, Value> Fields(Struct? extensions) =>
        extensions?.Fields ?? (IReadOnlyDictionary<string, Value>)new Dictionary<string, Value>();
}
