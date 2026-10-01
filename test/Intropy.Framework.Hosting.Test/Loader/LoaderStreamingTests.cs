using CloudNative.CloudEvents;
using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Testing.Delivery;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// What only the streaming transport has: the subscription the loader opens (its options, the
/// dead-letter topic), reopening a broken stream, waiting for a sidecar that starts late, and closing
/// the stream on shutdown. The behaviour both transports share is in <see cref="LoaderTests"/>.
/// </summary>
[Collection(ProcessStateCollection.Name)]
public class LoaderStreamingTests
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

    [Fact]
    public async Task Routing_PassesTheDeadLetterTopicAndRedeliversUnackedMessages()
    {
        await using var host = await LoaderHost.StartRoutingAsync();

        var subscription = host.Subscriber.Current;

        Assert.Equal(LoaderHost.PubSub, subscription.PubSubName);
        Assert.Equal(LoaderHost.Topic, subscription.TopicName);
        Assert.Equal("orders.dead", subscription.Options.DeadLetterTopic);
        Assert.Equal(TopicResponseAction.Retry, subscription.Options.MessageHandlingPolicy.DefaultResponseAction);
    }

    [Fact]
    public async Task BrokenStream_IsReopened()
    {
        await using var host = await LoaderHost.StartRoutingAsync();
        var first = host.Subscriber.Current;

        first.Break();
        await host.Subscriber.WaitForSubscriptionAsync(2);

        Assert.True(first.Disposed);
        var ack = await host.Subscriber.DeliverAsync(Event("ORD-1"));
        Assert.Equal(DeliveryAck.Success, ack);
    }

    [Fact]
    public async Task UnavailableSidecar_IsRetriedUntilTheSubscriptionOpens()
    {
        await using var host = await LoaderHost.StartAsync((services, fixture) =>
        {
            fixture.Subscriber.FailNextSubscribes(2);
            services.AddLoader<OrderCreated, OrderCreated, Context>(
                (b, _) => b.WithDeserializer(new JsonDeserializer<OrderCreated>())
                    .WithValidator(new TestValidator<OrderCreated>()).WithIdempotency()
                    .WithTransformer(new IdentityTransformer<OrderCreated>()).WithSender(fixture.CreatedSender)
                    .WithBusinessIncidents(_ => "id", _ => "subject"),
                (metadata, isRetry) => new Context(metadata, isRetry),
                o =>
                {
                    o.PubSubName = LoaderHost.PubSub;
                    o.TopicName = LoaderHost.Topic;
                });
        });

        Assert.Equal(3, host.Subscriber.SubscribeAttempts);
    }

    [Fact]
    public async Task Shutdown_ClosesTheStreamAfterTheMessageInFlight()
    {
        var host = await LoaderHost.StartRoutingAsync();
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var delivery = host.Subscriber.DeliverAsync(Event("ORD-1"));
        await host.CreatedSender.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var stopping = host.Host.StopAsync();
        host.CreatedSender.Gate.SetResult();

        Assert.Equal(DeliveryAck.Success, await delivery);
        await stopping;
        Assert.True(host.Subscriber.Current.Disposed);
        host.Host.Dispose();
    }
}
