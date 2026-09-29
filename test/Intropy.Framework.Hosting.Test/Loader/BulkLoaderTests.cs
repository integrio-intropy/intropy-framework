using System.Text;
using System.Text.Json;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Status = Dapr.AppCallback.Autogen.Grpc.v1.TopicEventResponse.Types.TopicEventResponseStatus;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// A loader with a batch route receives through Dapr bulk subscribe: it announces the bulk
/// subscription on its gRPC app callback, and answers every delivered batch with one status per
/// entry — batch routes running the batch at once, message routes one entry at a time.
/// </summary>
public class BulkLoaderTests
{
    private static string Created(string orderId, string customer = "CUST-1") =>
        JsonSerializer.Serialize(new OrderCreated(orderId, customer));

    private static string Cancelled(string orderId) =>
        JsonSerializer.Serialize(new OrderCancelled(orderId, "changed mind"));

    [Fact]
    public async Task ListTopicSubscriptions_AnnouncesTheBulkSubscription()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        var response = await host.Client.ListTopicSubscriptionsAsync();

        var subscription = Assert.Single(response.Subscriptions);
        Assert.Equal(LoaderHost.PubSub, subscription.PubsubName);
        Assert.Equal(LoaderHost.Topic, subscription.Topic);
        Assert.Equal("orders.dead", subscription.DeadLetterTopic);
        Assert.True(subscription.BulkSubscribe.Enabled);
        Assert.Equal(50, subscription.BulkSubscribe.MaxMessagesCount);
        Assert.Equal(500, subscription.BulkSubscribe.MaxAwaitDurationMs);
    }

    [Fact]
    public async Task BulkDelivery_RunsTheBatchRouteOncePerEntityWithOneLookup()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        var statuses = await host.DeliverAsync(
            BulkLoaderHost.Entry("e1", LoaderHost.Created, "ORD-1", Created("ORD-1"), minutes: 1),
            BulkLoaderHost.Entry("e2", LoaderHost.Created, "ORD-2", Created("ORD-2")),
            BulkLoaderHost.Entry("e3", LoaderHost.Created, "ORD-1", Created("ORD-1"), minutes: 2));

        Assert.All(statuses, s => Assert.Equal(Status.Success, s));
        Assert.Equal(["ORD-1", "ORD-2"], Assert.Single(host.Lookup.Calls).Order());
        Assert.Equal(["ORD-1", "ORD-2"], host.CreatedSender.Sent.Select(s => s.Value.OrderId).Order());
    }

    [Fact]
    public async Task BulkDelivery_RunsMessageRoutesPerEntryAndDeadLettersUnroutedEntries()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        var statuses = await host.DeliverAsync(
            BulkLoaderHost.Entry("e1", LoaderHost.Cancelled, "ORD-1", Cancelled("ORD-1")),
            BulkLoaderHost.Entry("e2", "order.shipped", "ORD-1", Created("ORD-1")),
            BulkLoaderHost.Entry("e3", LoaderHost.Cancelled, "ORD-2", Cancelled("ORD-2")));

        Assert.Equal([Status.Success, Status.Drop, Status.Success], statuses);
        Assert.Equal(["ORD-1", "ORD-2"], host.CancelledSender.Sent.Select(s => s.Value.OrderId));
    }

    [Fact]
    public async Task BulkDelivery_AcksFilteredEntriesWithoutLookingThemUp()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        var statuses = await host.DeliverAsync(
            BulkLoaderHost.Entry("e1", LoaderHost.Created, "ORD-1", Created("ORD-1", customer: "internal")));

        Assert.Equal([Status.Success], statuses);
        Assert.Empty(host.Lookup.Calls);
        Assert.Empty(host.CreatedSender.Sent);
    }

    [Fact]
    public async Task BulkDelivery_WhenTheLookupFails_RetriesTheBatchRoutesEntries()
    {
        await using var host = await BulkLoaderHost.StartAsync();
        host.Lookup.Fail = true;

        var statuses = await host.DeliverAsync(
            BulkLoaderHost.Entry("e1", LoaderHost.Created, "ORD-1", Created("ORD-1")),
            BulkLoaderHost.Entry("e2", LoaderHost.Cancelled, "ORD-2", Cancelled("ORD-2")));

        // Only the batch route depends on the lookup; the message route still loads.
        Assert.Equal([Status.Retry, Status.Success], statuses);
        Assert.DoesNotContain(host.Idempotency.Committed, m => m.Id.StartsWith(LoaderHost.Created, StringComparison.Ordinal));
    }

    [Fact]
    public async Task SingleDelivery_IsHandledAsABatchOfOne()
    {
        await using var host = await BulkLoaderHost.StartAsync();
        var extensions = new Struct();
        extensions.Fields["subject"] = Value.ForString("ORD-1");
        extensions.Fields["time"] = Value.ForString("2026-09-29T10:00:00Z");

        var response = await host.Client.OnTopicEventAsync(new TopicEventRequest
        {
            Id = "m1", Source = "urn:test", Type = LoaderHost.Created, SpecVersion = "1.0",
            DataContentType = "application/json", Data = ByteString.CopyFromUtf8(Created("ORD-1")),
            PubsubName = LoaderHost.PubSub, Topic = LoaderHost.Topic, Extensions = extensions
        });

        Assert.Equal(Status.Success, response.Status);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
    }

    [Fact]
    public async Task BulkDelivery_ReadsEntriesDeliveredAsStructuredCloudEvents()
    {
        await using var host = await BulkLoaderHost.StartAsync();
        var envelope = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["specversion"] = "1.0", ["id"] = "e1", ["source"] = "urn:test", ["type"] = LoaderHost.Cancelled,
            ["subject"] = "ORD-9", ["time"] = "2026-09-29T10:00:00Z", ["datacontenttype"] = "application/json",
            ["data"] = new OrderCancelled("ORD-9", "changed mind")
        });

        var statuses = await host.DeliverAsync(new TopicEventBulkRequestEntry
        {
            EntryId = "e1", ContentType = "application/cloudevents+json",
            Bytes = ByteString.CopyFrom(Encoding.UTF8.GetBytes(envelope))
        });

        Assert.Equal([Status.Success], statuses);
        Assert.Equal("ORD-9", Assert.Single(host.CancelledSender.Sent).Value.OrderId);
    }

    [Fact]
    public async Task LoaderWithABatchRoute_DoesNotOpenAStreamingSubscription()
    {
        await using var host = await BulkLoaderHost.StartAsync();

        await host.Client.ListTopicSubscriptionsAsync();

        Assert.Equal(0, host.Streaming.SubscribeAttempts);
    }
}
