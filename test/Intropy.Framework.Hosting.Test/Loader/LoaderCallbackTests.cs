using CloudNative.CloudEvents;
using Grpc.Core;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Testing.Delivery;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// What only the app-callback transport has: the gRPC server on the loader's port, no announced
/// subscription (a declarative resource owns it), deliveries the resource sent from the wrong topic,
/// and the time limit the loader enforces itself. The behaviour both transports share is in
/// <see cref="LoaderTests"/>.
/// </summary>
[Collection(ProcessStateCollection.Name)]
public class LoaderCallbackTests
{
    private static CloudEvent Event(string orderId) => new()
    {
        Id = Guid.NewGuid().ToString(),
        Source = new Uri("urn:test:source"),
        Type = LoaderHost.Created,
        Subject = orderId,
        Time = DateTimeOffset.UtcNow,
        DataContentType = "application/json",
        Data = new OrderCreated(orderId, "CUST-1")
    };

    private static Task<LoaderHost> StartRoutingAsync(Action<LoaderOptions>? configure = null) =>
        LoaderHost.StartRoutingAsync(configure, transport: LoaderTransport.AppCallback);

    [Fact]
    public async Task AppCallback_ServesTheCallbackAndAnnouncesNoSubscription()
    {
        await using var host = await StartRoutingAsync();

        var subscriptions = await host.Callback!.GetSubscriptionsAsync();

        Assert.Empty(subscriptions);
        Assert.Empty(host.Subscriber.Subscriptions);
    }

    [Fact]
    public async Task DefaultTransport_SubscribesByStreamingAndServesNothing()
    {
        var port = AppCallbackDelivery.AvailablePort();
        await using var host = await LoaderHost.StartRoutingAsync(o => o.CallbackPort = port);
        using var callback = new AppCallbackDelivery(port, LoaderHost.PubSub, LoaderHost.Topic);

        Assert.Single(host.Subscriber.Subscriptions);
        var refused = await Assert.ThrowsAsync<RpcException>(() => callback.GetSubscriptionsAsync());
        Assert.Equal(StatusCode.Unavailable, refused.StatusCode);
    }

    [Fact]
    public async Task AppCallback_WithoutCallbackPort_ServesOnAppPort()
    {
        var port = AppCallbackDelivery.AvailablePort();
        Environment.SetEnvironmentVariable("APP_PORT", port.ToString(System.Globalization.CultureInfo.InvariantCulture));
        try
        {
            await using var host = await StartRoutingAsync(o => o.CallbackPort = null);
            using var callback = new AppCallbackDelivery(port, LoaderHost.PubSub, LoaderHost.Topic);

            Assert.Empty(await callback.GetSubscriptionsAsync());
        }
        finally
        {
            Environment.SetEnvironmentVariable("APP_PORT", null);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(65536)]
    public void AppCallback_WithAnInvalidCallbackPort_FailsAtRegistration(int port)
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddLoader<OrderCreated, OrderCreated, Context>(
            (b, _) => b,
            (metadata, isRetry) => new Context(metadata, isRetry),
            o =>
            {
                o.PubSubName = LoaderHost.PubSub;
                o.TopicName = LoaderHost.Topic;
                o.Transport = LoaderTransport.AppCallback;
                o.CallbackPort = port;
            }));
    }

    [Theory]
    [InlineData("other-pubsub", LoaderHost.Topic)]
    [InlineData(LoaderHost.PubSub, "other-topic")]
    public async Task DeliveryForAnotherSubscription_IsLeftForRedeliveryWithoutProcessing(string pubSub, string topic)
    {
        await using var host = await StartRoutingAsync();

        var ack = await host.Callback!.DeliverAsync(Event("ORD-1"), pubSub, topic);

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.CreatedSender.Sent);
    }

    [Fact]
    public async Task MessageExceedingItsTimeLimit_IsCancelledAndLeftForRedelivery()
    {
        await using var host = await StartRoutingAsync(o => o.MaxMessageProcessingTime = TimeSpan.FromMilliseconds(200));
        // The sender never finishes on its own: only the loader's time limit ends it.
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var ack = await host.Callback!.DeliverAsync(Event("ORD-1")).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.CreatedSender.Sent);
    }
}
