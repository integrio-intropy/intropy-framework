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
/// sidecar does, one message per <c>OnTopicEvent</c> call, and returns the loader's acks.
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

    private async Task<DeliveryAck> SendAsync(CloudEvent cloudEvent, string pubSubName, string topicName,
        string path, bool redelivery, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(cloudEvent);
        var (data, attributes) = SidecarEncoding.Encode(cloudEvent);
        var extensions = new Struct();
        foreach (var (name, value) in attributes)
            extensions.Fields[name] = Value.ForString(value);
        if (redelivery)
            extensions.Fields["retrycount"] = Value.ForString("1");

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
            Extensions = extensions
        }, TopicEventResponse.Parser, ct);

        return response.Status switch
        {
            TopicEventResponse.Types.TopicEventResponseStatus.Success => DeliveryAck.Success,
            TopicEventResponse.Types.TopicEventResponseStatus.Drop => DeliveryAck.Drop,
            _ => DeliveryAck.Retry
        };
    }

    /// <inheritdoc />
    public void Dispose() => _channel.Dispose();

    // Dapr's protos ship only the server side of the AppCallback service, so the calls are described
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

/// <summary>A subscription an app announced to its sidecar.</summary>
/// <param name="PubSubName">The pub/sub component.</param>
/// <param name="TopicName">The topic.</param>
/// <param name="DeadLetterTopic">Where the sidecar drops messages to, if anywhere.</param>
public sealed record AnnouncedSubscription(string PubSubName, string TopicName, string? DeadLetterTopic);
