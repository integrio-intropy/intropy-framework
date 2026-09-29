using Intropy.Framework.Testing.Delivery;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// A loader with a batch route receives through Dapr bulk subscribe: it announces the bulk
/// subscription on its gRPC app callback, and answers every delivered batch with one ack per entry —
/// batch routes running the batch at once, message routes one entry at a time.
/// </summary>
public class BulkLoaderTests
{
    private static OrderCreated Created(string orderId, string customer = "CUST-1") => new(orderId, customer);

    private static OrderCancelled Cancelled(string orderId) => new(orderId, "changed mind");

    [Fact]
    public async Task Subscriptions_AnnounceTheBulkSubscription()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        var subscription = Assert.Single(await host.Delivery.GetSubscriptionsAsync());

        Assert.Equal(LoaderHost.PubSub, subscription.PubSubName);
        Assert.Equal(LoaderHost.Topic, subscription.TopicName);
        Assert.Equal("orders.dead", subscription.DeadLetterTopic);
        Assert.Equal(new AnnouncedBulk(50, TimeSpan.FromMilliseconds(500)), subscription.Bulk);
    }

    [Fact]
    public async Task BulkDelivery_RunsTheBatchRouteOncePerEntityWithOneLookup()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        var acks = await host.Delivery.DeliverBatchAsync(
            BulkLoaderHost.Event("e1", LoaderHost.Created, "ORD-1", Created("ORD-1"), minutes: 1),
            BulkLoaderHost.Event("e2", LoaderHost.Created, "ORD-2", Created("ORD-2")),
            BulkLoaderHost.Event("e3", LoaderHost.Created, "ORD-1", Created("ORD-1"), minutes: 2));

        Assert.All(acks, ack => Assert.Equal(DeliveryAck.Success, ack));
        Assert.Equal(["ORD-1", "ORD-2"], Assert.Single(host.Lookup.Calls).Order());
        Assert.Equal(["ORD-1", "ORD-2"], host.CreatedSender.Sent.Select(s => s.Value.OrderId).Order());
    }

    [Fact]
    public async Task BulkDelivery_RunsMessageRoutesPerEntryAndDeadLettersUnroutedEntries()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        var acks = await host.Delivery.DeliverBatchAsync(
            BulkLoaderHost.Event("e1", LoaderHost.Cancelled, "ORD-1", Cancelled("ORD-1")),
            BulkLoaderHost.Event("e2", "order.shipped", "ORD-1", Created("ORD-1")),
            BulkLoaderHost.Event("e3", LoaderHost.Cancelled, "ORD-2", Cancelled("ORD-2")));

        Assert.Equal([DeliveryAck.Success, DeliveryAck.Drop, DeliveryAck.Success], acks);
        Assert.Equal(["ORD-1", "ORD-2"], host.CancelledSender.Sent.Select(s => s.Value.OrderId));
    }

    [Fact]
    public async Task BulkDelivery_AcksFilteredEntriesWithoutLookingThemUp()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        var acks = await host.Delivery.DeliverBatchAsync(
            BulkLoaderHost.Event("e1", LoaderHost.Created, "ORD-1", Created("ORD-1", customer: "internal")));

        Assert.Equal([DeliveryAck.Success], acks);
        Assert.Empty(host.Lookup.Calls);
        Assert.Empty(host.CreatedSender.Sent);
    }

    [Fact]
    public async Task BulkDelivery_WhenTheLookupFails_RetriesTheBatchRoutesEntries()
    {
        await using var host = await BulkLoaderHost.StartAsync();
        host.Lookup.Fail = true;

        var acks = await host.Delivery.DeliverBatchAsync(
            BulkLoaderHost.Event("e1", LoaderHost.Created, "ORD-1", Created("ORD-1")),
            BulkLoaderHost.Event("e2", LoaderHost.Cancelled, "ORD-2", Cancelled("ORD-2")));

        // Only the batch route depends on the lookup; the message route still loads.
        Assert.Equal([DeliveryAck.Retry, DeliveryAck.Success], acks);
        Assert.DoesNotContain(host.Idempotency.Committed,
            m => m.Id.StartsWith(LoaderHost.Created, StringComparison.Ordinal));
    }

    [Fact]
    public async Task SingleDelivery_IsHandledAsABatchOfOne()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        var ack = await host.Delivery.DeliverAsync(
            BulkLoaderHost.Event("m1", LoaderHost.Created, "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
    }

    [Fact]
    public async Task BulkDelivery_ReadsEntriesDeliveredAsStructuredCloudEvents()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        var acks = await host.Delivery.DeliverBatchAsync(
            [BulkLoaderHost.Event("e1", LoaderHost.Cancelled, "ORD-9", Cancelled("ORD-9"))],
            topic: null, BulkEntryFormat.StructuredBytes);

        Assert.Equal([DeliveryAck.Success], acks);
        Assert.Equal("ORD-9", Assert.Single(host.CancelledSender.Sent).Value.OrderId);
    }

    [Fact]
    public async Task LoaderWithABatchRoute_DoesNotOpenAStreamingSubscription()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        await host.Delivery.GetSubscriptionsAsync();

        Assert.Equal(0, host.Streaming.SubscribeAttempts);
    }
}
