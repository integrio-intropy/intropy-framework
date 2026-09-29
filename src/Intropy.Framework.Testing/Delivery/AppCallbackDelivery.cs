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
/// Delivers <see cref="CloudEvent"/>s to a running batch loader's Dapr gRPC app callback exactly as the
/// sidecar does — the loader's subscription announcement, bulk deliveries and single deliveries — and
/// returns the loader's acks.
/// </summary>
/// <remarks>
/// Start the loader's host with <c>LoaderOptions.CallbackPort</c> set to a free port
/// (<see cref="AvailablePort"/>), then point the delivery at it. Deliveries go to the topic the loader
/// announces unless one is given.
/// </remarks>
public sealed class AppCallbackDelivery : IDisposable
{
    private static readonly string s_service = AppCallback.Descriptor.FullName;
    private readonly GrpcChannel _channel;
    private readonly CallInvoker _invoker;

    /// <summary>Connects to the app callback on <paramref name="port"/> of the local machine.</summary>
    public AppCallbackDelivery(int port) : this(new Uri($"http://127.0.0.1:{port}"))
    {
    }

    /// <summary>Connects to the app callback at <paramref name="address"/> (plain-text HTTP/2).</summary>
    public AppCallbackDelivery(Uri address)
    {
        ArgumentNullException.ThrowIfNull(address);
        _channel = GrpcChannel.ForAddress(address);
        _invoker = _channel.CreateCallInvoker();
    }

    /// <summary>A port nothing on the local machine listens on, for the loader's callback.</summary>
    public static int AvailablePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    /// <summary>Asks the loader for its subscriptions, as the sidecar does at startup.</summary>
    public async Task<IReadOnlyList<AnnouncedSubscription>> GetSubscriptionsAsync(CancellationToken ct = default)
    {
        var response = await CallAsync("ListTopicSubscriptions", new Empty(), ListTopicSubscriptionsResponse.Parser, ct);
        return [.. response.Subscriptions.Select(s => new AnnouncedSubscription(
            s.PubsubName,
            s.Topic,
            string.IsNullOrEmpty(s.DeadLetterTopic) ? null : s.DeadLetterTopic,
            s.BulkSubscribe is { Enabled: true } bulk
                ? new AnnouncedBulk(bulk.MaxMessagesCount, TimeSpan.FromMilliseconds(bulk.MaxAwaitDurationMs))
                : null))];
    }

    /// <summary>Delivers <paramref name="cloudEvents"/> as one bulk delivery on the loader's announced
    /// topic and returns each entry's ack, in order.</summary>
    public Task<IReadOnlyList<DeliveryAck>> DeliverBatchAsync(params CloudEvent[] cloudEvents) =>
        DeliverBatchAsync(cloudEvents, topic: null);

    /// <summary>Delivers <paramref name="cloudEvents"/> as one bulk delivery and returns each entry's
    /// ack, in order.</summary>
    /// <param name="cloudEvents">The events, as their publishers sent them.</param>
    /// <param name="topic">The pub/sub and topic to deliver on; defaults to the loader's single
    /// announced subscription.</param>
    /// <param name="format">How the entries are encoded: as CloudEvent attributes (the sidecar's
    /// usual form) or as structured-mode CloudEvents JSON bytes.</param>
    /// <param name="ct">Cancels the delivery.</param>
    public async Task<IReadOnlyList<DeliveryAck>> DeliverBatchAsync(IEnumerable<CloudEvent> cloudEvents,
        (string PubSubName, string TopicName)? topic, BulkEntryFormat format = BulkEntryFormat.CloudEvent,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(cloudEvents);
        var (pubSub, topicName) = topic ?? await AnnouncedTopicAsync(ct);
        var request = new TopicEventBulkRequest { Id = Guid.NewGuid().ToString(), PubsubName = pubSub, Topic = topicName };
        foreach (var cloudEvent in cloudEvents)
        {
            if (format == BulkEntryFormat.StructuredBytes)
            {
                request.Entries.Add(new TopicEventBulkRequestEntry
                {
                    EntryId = cloudEvent.Id ?? Guid.NewGuid().ToString(),
                    ContentType = DaprDelivery.CloudEventsContentType,
                    Bytes = ByteString.CopyFromUtf8(DaprDelivery.ToDeliveryEnvelope(cloudEvent))
                });
                continue;
            }

            var (data, extensions) = Encode(cloudEvent);
            request.Entries.Add(new TopicEventBulkRequestEntry
            {
                EntryId = cloudEvent.Id ?? Guid.NewGuid().ToString(),
                ContentType = "application/cloudevents+json",
                CloudEvent = new TopicEventCERequest
                {
                    Id = cloudEvent.Id ?? "", Source = cloudEvent.Source?.ToString() ?? "", Type = cloudEvent.Type ?? "",
                    SpecVersion = "1.0", DataContentType = cloudEvent.DataContentType ?? "application/json",
                    Data = data, Extensions = extensions
                }
            });
        }

        var response = await CallAsync("OnBulkTopicEvent", request, TopicEventBulkResponse.Parser, ct);
        var byEntry = response.Statuses.ToDictionary(s => s.EntryId, s => Ack(s.Status), StringComparer.Ordinal);
        return [.. request.Entries.Select(e => byEntry.TryGetValue(e.EntryId, out var ack) ? ack : DeliveryAck.Retry)];
    }

    /// <summary>Delivers one event outside a batch — as the sidecar does when it does not batch — and
    /// returns the loader's ack.</summary>
    public async Task<DeliveryAck> DeliverAsync(CloudEvent cloudEvent, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(cloudEvent);
        var (pubSub, topicName) = await AnnouncedTopicAsync(ct);
        var (data, extensions) = Encode(cloudEvent);
        var response = await CallAsync("OnTopicEvent", new TopicEventRequest
        {
            Id = cloudEvent.Id ?? Guid.NewGuid().ToString(), Source = cloudEvent.Source?.ToString() ?? "",
            Type = cloudEvent.Type ?? "", SpecVersion = "1.0",
            DataContentType = cloudEvent.DataContentType ?? "application/json", Data = data,
            PubsubName = pubSub, Topic = topicName, Extensions = extensions
        }, TopicEventResponse.Parser, ct);
        return Ack(response.Status);
    }

    /// <inheritdoc />
    public void Dispose() => _channel.Dispose();

    private async Task<(string, string)> AnnouncedTopicAsync(CancellationToken ct)
    {
        var subscriptions = await GetSubscriptionsAsync(ct);
        return subscriptions.Count == 1
            ? (subscriptions[0].PubSubName, subscriptions[0].TopicName)
            : throw new InvalidOperationException(
                $"The app announces {subscriptions.Count} subscriptions; name the topic to deliver on.");
    }

    private static (ByteString Data, Struct Extensions) Encode(CloudEvent cloudEvent)
    {
        var (data, attributes) = SidecarEncoding.Encode(cloudEvent);
        var extensions = new Struct();
        foreach (var (name, value) in attributes)
            extensions.Fields[name] = Value.ForString(value);
        return (ByteString.CopyFrom(data), extensions);
    }

    private static DeliveryAck Ack(TopicEventResponse.Types.TopicEventResponseStatus status) => status switch
    {
        TopicEventResponse.Types.TopicEventResponseStatus.Success => DeliveryAck.Success,
        TopicEventResponse.Types.TopicEventResponseStatus.Drop => DeliveryAck.Drop,
        _ => DeliveryAck.Retry
    };

    // Dapr.Protos ships only the server side of the AppCallback service, so the calls are described
    // from its service descriptor.
    private async Task<TResponse> CallAsync<TRequest, TResponse>(string method, TRequest request,
        MessageParser<TResponse> parser, CancellationToken ct)
        where TRequest : class, IMessage<TRequest>
        where TResponse : class, IMessage<TResponse>
    {
        var descriptor = new Method<TRequest, TResponse>(MethodType.Unary, s_service, method,
            Marshallers.Create<TRequest>(r => r.ToByteArray(), _ => throw new NotSupportedException()),
            Marshallers.Create(r => r.ToByteArray(), parser.ParseFrom));
        return await _invoker.AsyncUnaryCall(descriptor, null, new CallOptions(cancellationToken: ct), request);
    }
}

/// <summary>How <see cref="AppCallbackDelivery"/> encodes the entries of a bulk delivery.</summary>
public enum BulkEntryFormat
{
    /// <summary>As CloudEvent attributes, data and extensions — the sidecar's usual form.</summary>
    CloudEvent,

    /// <summary>As a structured-mode CloudEvents JSON envelope in the entry's bytes.</summary>
    StructuredBytes
}

/// <summary>A subscription an app announced to its sidecar.</summary>
/// <param name="PubSubName">The pub/sub component.</param>
/// <param name="TopicName">The topic.</param>
/// <param name="DeadLetterTopic">Where the sidecar drops messages to, if anywhere.</param>
/// <param name="Bulk">The batching, when the subscription is a bulk subscription.</param>
public sealed record AnnouncedSubscription(string PubSubName, string TopicName, string? DeadLetterTopic, AnnouncedBulk? Bulk);

/// <summary>A bulk subscription's batching.</summary>
/// <param name="MaxMessages">The most messages per delivery.</param>
/// <param name="MaxWait">How long the sidecar waits to fill a delivery.</param>
public sealed record AnnouncedBulk(int MaxMessages, TimeSpan MaxWait);
