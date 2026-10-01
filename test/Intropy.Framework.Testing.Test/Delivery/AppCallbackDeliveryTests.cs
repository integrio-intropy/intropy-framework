using System.Net;
using System.Net.Sockets;
using System.Text;
using CloudNative.CloudEvents;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Intropy.Framework.Testing.Delivery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Testing.Test.Delivery;

public class AppCallbackDeliveryTests
{
    private static CloudEvent Event(object data) => new()
    {
        Id = "e1",
        Source = new Uri("urn:test"),
        Type = "order.created",
        Subject = "ORD-1",
        Time = new DateTimeOffset(2026, 9, 29, 10, 0, 0, TimeSpan.Zero),
        DataContentType = "application/json",
        Data = data
    };

    [Fact]
    public async Task DeliverAsync_EncodesTheEventAsTheSidecarDoes()
    {
        await using var app = await RecordingApp.StartAsync();
        using var delivery = new AppCallbackDelivery(app.Port, "pubsub", "orders");

        await delivery.DeliverAsync(Event(new { OrderId = "ORD-1" }));

        var request = Assert.Single(app.Requests);
        Assert.Equal("e1", request.Id);
        Assert.Equal("urn:test", request.Source);
        Assert.Equal("order.created", request.Type);
        Assert.Equal("application/json", request.DataContentType);
        Assert.Equal("pubsub", request.PubsubName);
        Assert.Equal("orders", request.Topic);
        Assert.Equal("""{"OrderId":"ORD-1"}""", request.Data.ToStringUtf8());
        Assert.Equal("ORD-1", request.Extensions.Fields["subject"].StringValue);
        Assert.Equal("2026-09-29T10:00:00Z", request.Extensions.Fields["time"].StringValue);
        Assert.False(request.Extensions.Fields.ContainsKey("retrycount"));
    }

    [Fact]
    public async Task DeliverAsync_WithAPayloadPublishedAsAJsonString_DeliversItQuoted()
    {
        await using var app = await RecordingApp.StartAsync();
        using var delivery = new AppCallbackDelivery(app.Port, "pubsub", "orders");

        await delivery.DeliverAsync(Event("""{"OrderId":"ORD-1"}"""));

        Assert.Equal("\"{\\u0022OrderId\\u0022:\\u0022ORD-1\\u0022}\"",
            Encoding.UTF8.GetString(Assert.Single(app.Requests).Data.Span));
    }

    [Fact]
    public async Task DeliverAsync_AsARedelivery_AddsTheRetryCount()
    {
        await using var app = await RecordingApp.StartAsync();
        using var delivery = new AppCallbackDelivery(app.Port, "pubsub", "orders");

        await delivery.DeliverAsync(Event(new { OrderId = "ORD-1" }), redelivery: true);

        Assert.True(Assert.Single(app.Requests).Extensions.Fields.ContainsKey("retrycount"));
    }

    [Fact]
    public async Task DeliverAsync_OnAnotherSubscription_DeliversFromThatPubSubAndTopic()
    {
        await using var app = await RecordingApp.StartAsync();
        using var delivery = new AppCallbackDelivery(app.Port, "pubsub", "orders");

        await delivery.DeliverAsync(Event(new { }), "other-pubsub", "other-topic");

        var request = Assert.Single(app.Requests);
        Assert.Equal("other-pubsub", request.PubsubName);
        Assert.Equal("other-topic", request.Topic);
    }

    [Theory]
    [InlineData(TopicEventResponse.Types.TopicEventResponseStatus.Success, DeliveryAck.Success)]
    [InlineData(TopicEventResponse.Types.TopicEventResponseStatus.Retry, DeliveryAck.Retry)]
    [InlineData(TopicEventResponse.Types.TopicEventResponseStatus.Drop, DeliveryAck.Drop)]
    public async Task DeliverAsync_ReturnsTheAppsStatusAsAnAck(TopicEventResponse.Types.TopicEventResponseStatus status,
        DeliveryAck expected)
    {
        await using var app = await RecordingApp.StartAsync(status);
        using var delivery = new AppCallbackDelivery(app.Port, "pubsub", "orders");

        var ack = await delivery.DeliverAsync(Event(new { }));

        Assert.Equal(expected, ack);
    }

    [Fact]
    public async Task GetSubscriptionsAsync_ReturnsWhatTheAppAnnounces()
    {
        await using var app = await RecordingApp.StartAsync(announce: new TopicSubscription
        {
            PubsubName = "pubsub", Topic = "orders", DeadLetterTopic = "orders.dead"
        });
        using var delivery = new AppCallbackDelivery(app.Port, "pubsub", "orders");

        var subscription = Assert.Single(await delivery.GetSubscriptionsAsync());

        Assert.Equal(new AnnouncedSubscription("pubsub", "orders", "orders.dead"), subscription);
    }

    [Fact]
    public async Task GetSubscriptionsAsync_WhenTheAppAnnouncesNone_ReturnsNone()
    {
        await using var app = await RecordingApp.StartAsync();
        using var delivery = new AppCallbackDelivery(app.Port, "pubsub", "orders");

        Assert.Empty(await delivery.GetSubscriptionsAsync());
    }

    [Fact]
    public async Task DeliverAsync_WhenNothingListens_FailsAsUnavailable()
    {
        using var delivery = new AppCallbackDelivery(AppCallbackDelivery.AvailablePort(), "pubsub", "orders");

        var error = await Assert.ThrowsAsync<RpcException>(() => delivery.DeliverAsync(Event(new { })));

        Assert.Equal(StatusCode.Unavailable, error.StatusCode);
    }

    [Fact]
    public void AvailablePort_ReturnsAPortThatCanBeListenedOn()
    {
        var port = AppCallbackDelivery.AvailablePort();

        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
    }

    [Theory]
    [InlineData("", "orders")]
    [InlineData("pubsub", " ")]
    public void Constructor_WithoutAPubSubOrTopic_Throws(string pubSub, string topic)
    {
        Assert.ThrowsAny<ArgumentException>(() => new AppCallbackDelivery(5000, pubSub, topic));
    }

    /// <summary>A gRPC app callback that records what it is delivered and answers with a fixed
    /// status — standing in for a loader.</summary>
    private sealed class RecordingApp : AppCallback.AppCallbackBase, IAsyncDisposable
    {
        private readonly List<TopicEventRequest> _requests = [];
        private TopicEventResponse.Types.TopicEventResponseStatus _status;
        private TopicSubscription? _announce;
        private WebApplication _server = null!;

        public int Port { get; private set; }

        public IReadOnlyList<TopicEventRequest> Requests
        {
            get { lock (_requests) return [.. _requests]; }
        }

        public static async Task<RecordingApp> StartAsync(
            TopicEventResponse.Types.TopicEventResponseStatus status = TopicEventResponse.Types.TopicEventResponseStatus.Success,
            TopicSubscription? announce = null)
        {
            var app = new RecordingApp { _status = status, _announce = announce, Port = AppCallbackDelivery.AvailablePort() };
            var builder = WebApplication.CreateSlimBuilder();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.ListenLocalhost(app.Port, listen => listen.Protocols = HttpProtocols.Http2));
            builder.Logging.ClearProviders();
            builder.Services.AddGrpc();
            builder.Services.AddSingleton(app);
            app._server = builder.Build();
            app._server.MapGrpcService<RecordingApp>();
            await app._server.StartAsync();
            return app;
        }

        public override Task<ListTopicSubscriptionsResponse> ListTopicSubscriptions(Empty request,
            ServerCallContext context)
        {
            var response = new ListTopicSubscriptionsResponse();
            if (_announce is not null)
                response.Subscriptions.Add(_announce);
            return Task.FromResult(response);
        }

        public override Task<TopicEventResponse> OnTopicEvent(TopicEventRequest request, ServerCallContext context)
        {
            lock (_requests) _requests.Add(request);
            return Task.FromResult(new TopicEventResponse { Status = _status });
        }

        public async ValueTask DisposeAsync() => await _server.DisposeAsync();
    }
}
