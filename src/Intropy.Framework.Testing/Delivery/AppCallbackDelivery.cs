using System.Net;
using System.Net.Sockets;
using CloudNative.CloudEvents;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

namespace Intropy.Framework.Testing.Delivery;

/// <summary>
/// Delivers <see cref="CloudEvent"/>s to a running loader's Dapr gRPC app callback exactly as the
/// sidecar does — one message per <c>OnTopicEvent</c> call, or, for a subscription that declares
/// <c>bulkSubscribe</c>, a whole batch per <c>OnBulkTopicEvent</c> call — and returns the loader's
/// acks.
/// </summary>
/// <remarks>
/// The loader always serves the Dapr gRPC app callback: start its host with
/// <c>LoaderOptions.CallbackPort</c> set to a free port (<see cref="AvailablePort"/>), then point the
/// delivery at it. The loader announces no subscription — its subscription is a declarative
/// resource — so the delivery is told which pub/sub and topic it delivers on, as the resource tells
/// the sidecar.
/// </remarks>
public sealed class AppCallbackDelivery : IDisposable
{
    private static readonly string s_service = AppCallback.Descriptor.FullName;
    // The rendered Subscription's default route (the topology's SubscriptionRouting.UnhandledPath).
    private const string UnhandledPath = "/unhandled";

    private readonly GrpcChannel _channel;
    private readonly CallInvoker _invoker;

    /// <summary>Connects to the app callback on <paramref name="port"/> of the local machine.</summary>
    /// <param name="port">The loader's callback port.</param>
    /// <param name="pubSubName">The pub/sub component deliveries come from.</param>
    /// <param name="topicName">The topic deliveries come from.</param>
    public AppCallbackDelivery(int port, string pubSubName, string topicName)
        : this(new Uri($"http://127.0.0.1:{port}"), pubSubName, topicName)
    {
    }

    /// <summary>Connects to the app callback at <paramref name="address"/> (plain-text HTTP/2).</summary>
    /// <param name="address">The loader's callback address.</param>
    /// <param name="pubSubName">The pub/sub component deliveries come from.</param>
    /// <param name="topicName">The topic deliveries come from.</param>
    public AppCallbackDelivery(Uri address, string pubSubName, string topicName)
    {
        ArgumentNullException.ThrowIfNull(address);
        ArgumentException.ThrowIfNullOrWhiteSpace(pubSubName);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        PubSubName = pubSubName;
        TopicName = topicName;
        _channel = GrpcChannel.ForAddress(address);
        _invoker = _channel.CreateCallInvoker();
    }

    /// <summary>The pub/sub component deliveries come from.</summary>
    public string PubSubName { get; }

    /// <summary>The topic deliveries come from.</summary>
    public string TopicName { get; }

    /// <summary>A port nothing on the local machine listens on, for the loader's callback.</summary>
    public static int AvailablePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    /// <summary>Asks the app for the subscriptions it announces, as the sidecar does at startup.</summary>
    public async Task<IReadOnlyList<AnnouncedSubscription>> GetSubscriptionsAsync(CancellationToken ct = default)
    {
        var response = await CallAsync("ListTopicSubscriptions", new Empty(), ListTopicSubscriptionsResponse.Parser, ct);
        return [.. response.Subscriptions.Select(s => new AnnouncedSubscription(
            s.PubsubName,
            s.Topic,
            string.IsNullOrEmpty(s.DeadLetterTopic) ? null : s.DeadLetterTopic))];
    }

    /// <summary>Delivers <paramref name="cloudEvent"/> on <see cref="PubSubName"/> and
    /// <see cref="TopicName"/> and returns the loader's ack.</summary>
    /// <param name="cloudEvent">The event, as its publisher sent it.</param>
    /// <param name="redelivery">Marks the delivery as a redelivery (the sidecar's <c>retrycount</c>
    /// extension).</param>
    /// <param name="ct">Cancels the call, as the sidecar would when it gives up on it.</param>
    public Task<DeliveryAck> DeliverAsync(CloudEvent cloudEvent, bool redelivery = false,
        CancellationToken ct = default) => DeliverAsync(cloudEvent, PubSubName, TopicName, redelivery, ct);

    /// <summary>Delivers <paramref name="cloudEvent"/> on the subscription's default route, as the
    /// sidecar does when none of the subscription's rules select it — a content filter left it
    /// out — and returns the block's ack.</summary>
    /// <param name="cloudEvent">The event, as its publisher sent it.</param>
    /// <param name="redelivery">Marks the delivery as a redelivery.</param>
    /// <param name="ct">Cancels the call.</param>
    public Task<DeliveryAck> DeliverUnhandledAsync(CloudEvent cloudEvent, bool redelivery = false,
        CancellationToken ct = default) =>
        SendAsync(cloudEvent, PubSubName, TopicName, UnhandledPath, redelivery, ct);

    /// <summary>Delivers <paramref name="cloudEvent"/> on the given pub/sub and topic and returns the
    /// loader's ack.</summary>
    /// <param name="cloudEvent">The event, as its publisher sent it.</param>
    /// <param name="pubSubName">The pub/sub component the delivery comes from.</param>
    /// <param name="topicName">The topic the delivery comes from.</param>
    /// <param name="redelivery">Marks the delivery as a redelivery.</param>
    /// <param name="ct">Cancels the call.</param>
    public Task<DeliveryAck> DeliverAsync(CloudEvent cloudEvent, string pubSubName, string topicName,
        bool redelivery = false, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(cloudEvent);
        // The route the rendered subscription's rule for the event's type delivers it on.
        return SendAsync(cloudEvent, pubSubName, topicName, $"/{cloudEvent.Type}", redelivery, ct);
    }

    /// <summary>Delivers <paramref name="cloudEvents"/> as one bulk delivery on
    /// <see cref="PubSubName"/> and <see cref="TopicName"/>, as the sidecar does for a subscription
    /// that declares <c>bulkSubscribe</c>: one <c>OnBulkTopicEvent</c> call for the whole batch, and
    /// one ack per event back.</summary>
    /// <remarks>The sidecar groups a batch by the route its rules selected, so a real batch carries
    /// events of one type and is delivered on that type's route — the route of the first event
    /// here. Mixing types reproduces a delivery the sidecar does not make.</remarks>
    /// <param name="cloudEvents">The batch's events, as their publishers sent them.</param>
    /// <param name="redelivery">Marks every entry as a redelivery.</param>
    /// <param name="ct">Cancels the call, as the sidecar would when it gives up on it.</param>
    /// <returns>The block's ack for each event, in the order the events were given.</returns>
    public Task<IReadOnlyList<DeliveryAck>> DeliverBulkAsync(IReadOnlyList<CloudEvent> cloudEvents,
        bool redelivery = false, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(cloudEvents);
        if (cloudEvents.Count == 0)
            throw new ArgumentException("A bulk delivery carries at least one event.", nameof(cloudEvents));
        return SendBulkAsync(cloudEvents, $"/{cloudEvents[0].Type}", redelivery, ct);
    }

    /// <summary>Delivers <paramref name="cloudEvents"/> as one bulk delivery on the subscription's
    /// default route, as the sidecar does when none of the subscription's rules select them, and
    /// returns the block's ack for each.</summary>
    /// <param name="cloudEvents">The batch's events, as their publishers sent them.</param>
    /// <param name="redelivery">Marks every entry as a redelivery.</param>
    /// <param name="ct">Cancels the call.</param>
    /// <returns>The block's ack for each event, in the order the events were given.</returns>
    public Task<IReadOnlyList<DeliveryAck>> DeliverBulkUnhandledAsync(IReadOnlyList<CloudEvent> cloudEvents,
        bool redelivery = false, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(cloudEvents);
        if (cloudEvents.Count == 0)
            throw new ArgumentException("A bulk delivery carries at least one event.", nameof(cloudEvents));
        return SendBulkAsync(cloudEvents, UnhandledPath, redelivery, ct);
    }

    private async Task<DeliveryAck> SendAsync(CloudEvent cloudEvent, string pubSubName, string topicName,
        string path, bool redelivery, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(cloudEvent);
        var (data, attributes) = SidecarEncoding.Encode(cloudEvent);

        var response = await CallAsync("OnTopicEvent", new TopicEventRequest
        {
            Id = cloudEvent.Id ?? Guid.NewGuid().ToString(),
            Source = cloudEvent.Source?.ToString() ?? "",
            Type = cloudEvent.Type ?? "",
            SpecVersion = "1.0",
            DataContentType = cloudEvent.DataContentType ?? "application/json",
            Data = ByteString.CopyFrom(data),
            PubsubName = pubSubName,
            Topic = topicName,
            Path = path,
            Extensions = Extensions(attributes, redelivery)
        }, TopicEventResponse.Parser, ct);

        return Ack(response.Status);
    }

    private async Task<IReadOnlyList<DeliveryAck>> SendBulkAsync(IReadOnlyList<CloudEvent> cloudEvents, string path,
        bool redelivery, CancellationToken ct)
    {
        // The batch-wide type the sidecar sets on every bulk delivery; the events' own types are on
        // their entries.
        var request = new TopicEventBulkRequest
        {
            Id = Guid.NewGuid().ToString(),
            PubsubName = PubSubName,
            Topic = TopicName,
            Path = path,
            Type = "com.dapr.event.sent.bulk"
        };
        // The sidecar also sends each entry's trace context as call metadata: one traceparent and
        // one grpc-trace-bin header per entry, all-zero for an entry without one.
        var headers = new Metadata();
        var entryIds = new List<string>(cloudEvents.Count);
        foreach (var cloudEvent in cloudEvents)
        {
            ArgumentNullException.ThrowIfNull(cloudEvent);
            var (data, attributes) = SidecarEncoding.Encode(cloudEvent);
            var traceParent = attributes.GetValueOrDefault("traceparent") ?? NoTraceParent;
            headers.Add("traceparent", traceParent);
            headers.Add("grpc-trace-bin", TraceContextBinary(traceParent));
            var entryId = Guid.NewGuid().ToString();
            entryIds.Add(entryId);
            request.Entries.Add(new TopicEventBulkRequestEntry
            {
                EntryId = entryId,
                ContentType = "application/cloudevents+json",
                CloudEvent = new TopicEventCERequest
                {
                    Id = cloudEvent.Id ?? Guid.NewGuid().ToString(),
                    Source = cloudEvent.Source?.ToString() ?? "",
                    Type = cloudEvent.Type ?? "",
                    SpecVersion = "1.0",
                    DataContentType = cloudEvent.DataContentType ?? "application/json",
                    Data = ByteString.CopyFrom(data),
                    Extensions = Extensions(attributes, redelivery)
                }
            });
        }

        var response = await CallAsync("OnBulkTopicEvent", request, TopicEventBulkResponse.Parser, ct, headers);

        // The sidecar retries an entry the app answered nothing for.
        var acks = response.Statuses.ToDictionary(s => s.EntryId, s => Ack(s.Status), StringComparer.Ordinal);
        return [.. entryIds.Select(id => acks.GetValueOrDefault(id, DeliveryAck.Retry))];
    }

    private const string NoTraceParent = "00-00000000000000000000000000000000-0000000000000000-00";

    /// <summary>A W3C traceparent in the binary form gRPC carries it in: version, then the trace
    /// id, span id and flags, each behind its field id.</summary>
    private static byte[] TraceContextBinary(string traceParent)
    {
        var parts = traceParent.Split('-');
        return [0, 0, .. Convert.FromHexString(parts[1]), 1, .. Convert.FromHexString(parts[2]), 2,
            .. Convert.FromHexString(parts[3])];
    }

    private static Struct Extensions(IReadOnlyDictionary<string, string> attributes, bool redelivery)
    {
        var extensions = new Struct();
        foreach (var (name, value) in attributes)
            extensions.Fields[name] = Value.ForString(value);
        if (redelivery)
            extensions.Fields["retrycount"] = Value.ForString("1");
        return extensions;
    }

    private static DeliveryAck Ack(TopicEventResponse.Types.TopicEventResponseStatus status) => status switch
    {
        TopicEventResponse.Types.TopicEventResponseStatus.Success => DeliveryAck.Success,
        TopicEventResponse.Types.TopicEventResponseStatus.Drop => DeliveryAck.Drop,
        _ => DeliveryAck.Retry
    };

    /// <inheritdoc />
    public void Dispose() => _channel.Dispose();

    // Dapr's protos ship only the server side of the AppCallback service, so the calls are described
    // from its service descriptor.
    private async Task<TResponse> CallAsync<TRequest, TResponse>(string method, TRequest request,
        MessageParser<TResponse> parser, CancellationToken ct, Metadata? headers = null)
        where TRequest : class, IMessage<TRequest>
        where TResponse : class, IMessage<TResponse>
    {
        var descriptor = new Method<TRequest, TResponse>(MethodType.Unary, s_service, method,
            Marshallers.Create<TRequest>(r => r.ToByteArray(), _ => throw new NotSupportedException()),
            Marshallers.Create(r => r.ToByteArray(), parser.ParseFrom));
        return await _invoker.AsyncUnaryCall(descriptor, null, new CallOptions(headers, cancellationToken: ct), request);
    }
}

/// <summary>A subscription an app announced to its sidecar.</summary>
/// <param name="PubSubName">The pub/sub component.</param>
/// <param name="TopicName">The topic.</param>
/// <param name="DeadLetterTopic">Where the sidecar drops messages to, if anywhere.</param>
public sealed record AnnouncedSubscription(string PubSubName, string TopicName, string? DeadLetterTopic);
