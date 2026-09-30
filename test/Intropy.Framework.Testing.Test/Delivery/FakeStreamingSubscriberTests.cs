using System.Text;
using CloudNative.CloudEvents;
using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Testing.Delivery;

namespace Intropy.Framework.Testing.Test.Delivery;

public class FakeStreamingSubscriberTests
{
    private static readonly DaprSubscriptionOptions s_options =
        new(new MessageHandlingPolicy(TimeSpan.FromSeconds(30), TopicResponseAction.Retry)) { DeadLetterTopic = "dead" };

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
    public async Task SubscribeAsync_RecordsTheSubscription()
    {
        var subscriber = new FakeStreamingSubscriber();

        await subscriber.SubscribeAsync("pubsub", "orders", s_options, (_, _) => Task.FromResult(TopicResponseAction.Success),
            CancellationToken.None);

        var subscription = Assert.Single(subscriber.Subscriptions);
        Assert.Same(subscription, subscriber.Current);
        Assert.Equal("pubsub", subscription.PubSubName);
        Assert.Equal("orders", subscription.TopicName);
        Assert.Equal("dead", subscription.Options.DeadLetterTopic);
    }

    [Theory]
    [InlineData(TopicResponseAction.Success, DeliveryAck.Success)]
    [InlineData(TopicResponseAction.Retry, DeliveryAck.Retry)]
    [InlineData(TopicResponseAction.Drop, DeliveryAck.Drop)]
    public async Task DeliverAsync_ReturnsTheHandlersResponseAsAnAck(TopicResponseAction response, DeliveryAck expected)
    {
        var subscriber = new FakeStreamingSubscriber();
        await subscriber.SubscribeAsync("pubsub", "orders", s_options, (_, _) => Task.FromResult(response),
            CancellationToken.None);

        var ack = await subscriber.DeliverAsync(Event(new { OrderId = "ORD-1" }));

        Assert.Equal(expected, ack);
    }

    [Fact]
    public async Task DeliverAsync_EncodesTheEventAsTheSidecarDoes()
    {
        TopicMessage? delivered = null;
        var subscriber = await SubscribedAsync(m => delivered = m);

        await subscriber.DeliverAsync(Event(new { OrderId = "ORD-1" }));

        Assert.NotNull(delivered);
        Assert.Equal("e1", delivered.Id);
        Assert.Equal("order.created", delivered.Type);
        Assert.Equal("orders", delivered.Topic);
        Assert.Equal("""{"OrderId":"ORD-1"}""", Encoding.UTF8.GetString(delivered.Data.Span));
        Assert.Equal("ORD-1", delivered.Extensions["subject"].StringValue);
        Assert.Equal("2026-09-29T10:00:00Z", delivered.Extensions["time"].StringValue);
        Assert.False(delivered.Extensions.ContainsKey("retrycount"));
    }

    [Fact]
    public async Task DeliverAsync_WithAPayloadPublishedAsAJsonString_DeliversItQuoted()
    {
        TopicMessage? delivered = null;
        var subscriber = await SubscribedAsync(m => delivered = m);

        await subscriber.DeliverAsync(Event("""{"OrderId":"ORD-1"}"""));

        Assert.Equal("\"{\\u0022OrderId\\u0022:\\u0022ORD-1\\u0022}\"", Encoding.UTF8.GetString(delivered!.Data.Span));
    }

    [Fact]
    public async Task DeliverAsync_AsARedelivery_AddsTheRetryCount()
    {
        TopicMessage? delivered = null;
        var subscriber = await SubscribedAsync(m => delivered = m);

        await subscriber.DeliverAsync(Event(new { OrderId = "ORD-1" }), redelivery: true);

        Assert.True(delivered!.Extensions.ContainsKey("retrycount"));
    }

    [Fact]
    public async Task DeliverAsync_BeforeTheLoaderSubscribed_Throws()
    {
        var subscriber = new FakeStreamingSubscriber();

        await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.DeliverAsync(Event(new { })));
    }

    [Fact]
    public async Task FailNextSubscribes_FailsThatManyAttemptsThenSubscribes()
    {
        var subscriber = new FakeStreamingSubscriber();
        subscriber.FailNextSubscribes(2);

        for (var i = 0; i < 2; i++)
            await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.SubscribeAsync("pubsub", "orders",
                s_options, (_, _) => Task.FromResult(TopicResponseAction.Success), CancellationToken.None));
        await subscriber.SubscribeAsync("pubsub", "orders", s_options,
            (_, _) => Task.FromResult(TopicResponseAction.Success), CancellationToken.None);

        Assert.Equal(3, subscriber.SubscribeAttempts);
        Assert.Single(subscriber.Subscriptions);
    }

    [Fact]
    public async Task WaitForSubscriptionAsync_WhenTheLoaderNeverSubscribes_TimesOut()
    {
        var subscriber = new FakeStreamingSubscriber();

        await Assert.ThrowsAsync<TimeoutException>(() =>
            subscriber.WaitForSubscriptionAsync(timeout: TimeSpan.FromMilliseconds(50)));
    }

    [Fact]
    public async Task Break_EndsTheSubscriptionsCompletionWithAFault()
    {
        var subscriber = await SubscribedAsync(_ => { });

        subscriber.Current.Break();

        await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.Current.Completion);
        Assert.False(subscriber.Current.Disposed);
    }

    [Fact]
    public async Task DisposeAsync_ClosesTheSubscription()
    {
        var subscriber = await SubscribedAsync(_ => { });

        await subscriber.Current.DisposeAsync();

        Assert.True(subscriber.Current.Disposed);
        Assert.True(subscriber.Current.Completion.IsCompletedSuccessfully);
    }

    private static async Task<FakeStreamingSubscriber> SubscribedAsync(Action<TopicMessage> onMessage)
    {
        var subscriber = new FakeStreamingSubscriber();
        await subscriber.SubscribeAsync("pubsub", "orders", s_options, (message, _) =>
        {
            onMessage(message);
            return Task.FromResult(TopicResponseAction.Success);
        }, CancellationToken.None);
        return subscriber;
    }
}
