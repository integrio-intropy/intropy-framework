using System.Diagnostics;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Testing.Delivery;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// A bulk loader end to end over its real app callback: a batch pushed the way the sidecar pushes a
/// bulkSubscribe delivery, through the real consumer, route and bulk pipeline, with one ack per
/// entry back.
/// </summary>
[Collection(ProcessStateCollection.Name)]
public class BulkLoaderCallbackTests
{
    private static readonly CloudEventAttribute s_dataRef =
        CloudEventAttribute.CreateExtension(CloudEventExtensionKeys.DataRef, CloudEventAttributeType.String);

    private static readonly CloudEventAttribute s_traceParent =
        CloudEventAttribute.CreateExtension("traceparent", CloudEventAttributeType.String);

    private static CloudEvent Created(string orderId) => new()
    {
        Id = Guid.NewGuid().ToString(),
        Source = new Uri("urn:test:source"),
        Type = LoaderHost.Created,
        Subject = orderId,
        Time = DateTimeOffset.UtcNow,
        DataContentType = "application/json",
        Data = new OrderCreated(orderId, "CUST-1")
    };

    private static CloudEvent Cancelled(string orderId) => new()
    {
        Id = Guid.NewGuid().ToString(),
        Source = new Uri("urn:test:source"),
        Type = LoaderHost.Cancelled,
        Subject = orderId,
        Time = DateTimeOffset.UtcNow,
        DataContentType = "application/json",
        Data = new OrderCancelled(orderId, "changed my mind")
    };

    [Fact]
    public async Task ABatch_IsSentInOneCall_AndEveryEntryAcknowledged()
    {
        await using var host = await BulkLoaderHost.StartRoutingAsync();

        var acks = await host.DeliverBulkAsync(Created("ORD-1"), Created("ORD-2"), Created("ORD-3"));

        Assert.Equal([DeliveryAck.Success, DeliveryAck.Success, DeliveryAck.Success], acks);
        var batch = Assert.Single(host.CreatedSender.Batches);
        Assert.Equal(["ORD-1", "ORD-2", "ORD-3"], batch.Select(o => o.OrderId));
    }

    [Fact]
    public async Task ABatch_GoesToTheRouteForItsEventType()
    {
        await using var host = await BulkLoaderHost.StartRoutingAsync();

        var acks = await host.DeliverBulkAsync(Cancelled("ORD-1"), Cancelled("ORD-2"));

        Assert.All(acks, ack => Assert.Equal(DeliveryAck.Success, ack));
        Assert.Equal(2, Assert.Single(host.CancelledSender.Batches).Count);
        Assert.Empty(host.CreatedSender.Batches);
    }

    [Fact]
    public async Task ABatchOfOne_IsLoadedLikeAnyOther()
    {
        // A route whose messages rarely queue up — a self-triggered command — arrives in batches of
        // one on a bulk subscription.
        await using var host = await BulkLoaderHost.StartRoutingAsync();

        var acks = await host.DeliverBulkAsync(Cancelled("ORD-1"));

        Assert.Equal([DeliveryAck.Success], acks);
        Assert.Single(Assert.Single(host.CancelledSender.Batches));
    }

    [Fact]
    public async Task AMessageThatCannotBePrepared_FailsAlone_AndTheRestOfTheBatchIsLoaded()
    {
        // A claim pointer this framework never mints: resolving it fails for that message only. It
        // must not send the batch's other messages back for redelivery with it.
        await using var host = await BulkLoaderHost.StartRoutingAsync();
        var poisoned = Created("ORD-2");
        poisoned[s_dataRef] = "../../outside/the/claim/store";

        var acks = await host.DeliverBulkAsync(Created("ORD-1"), poisoned, Created("ORD-3"));

        Assert.Equal([DeliveryAck.Success, DeliveryAck.Retry, DeliveryAck.Success], acks);
        var batch = Assert.Single(host.CreatedSender.Batches);
        Assert.Equal(["ORD-1", "ORD-3"], batch.Select(o => o.OrderId));
    }

    [Fact]
    public async Task ARejectedMessage_IsAcknowledgedAndRoutedAsAnIncident_WithoutHoldingUpTheBatch()
    {
        // The same contract as a single-message loader, per entry: a terminal business failure is
        // handled by routing its incident, so it is acknowledged and never sent.
        await using var host = await BulkLoaderHost.StartRoutingAsync(reject: o => o.OrderId == "ORD-BAD");

        var acks = await host.DeliverBulkAsync(Created("ORD-1"), Created("ORD-BAD"), Created("ORD-3"));

        Assert.All(acks, ack => Assert.Equal(DeliveryAck.Success, ack));
        Assert.Single(host.Incidents.Incidents);
        Assert.Equal(["ORD-1", "ORD-3"], Assert.Single(host.CreatedSender.Batches).Select(o => o.OrderId));
    }

    [Fact]
    public async Task ABatchTheRulesSelectedNoRouteFor_IsLeftForRedelivery_ByDefault()
    {
        await using var host = await BulkLoaderHost.StartRoutingAsync();

        var acks = await host.Callback.DeliverBulkUnhandledAsync([Created("ORD-1"), Created("ORD-2")]);

        Assert.All(acks, ack => Assert.Equal(DeliveryAck.Retry, ack));
        Assert.Empty(host.CreatedSender.Batches);
    }

    [Fact]
    public async Task ABatchTheRulesSelectedNoRouteFor_IsAcknowledged_WhenTheLoaderDropsUnrouted()
    {
        await using var host = await BulkLoaderHost.StartRoutingAsync(configure: o => o.Unrouted = UnroutedPolicy.Ack);

        var acks = await host.Callback.DeliverBulkUnhandledAsync([Created("ORD-1"), Created("ORD-2")]);

        Assert.All(acks, ack => Assert.Equal(DeliveryAck.Success, ack));
        Assert.Empty(host.CreatedSender.Batches);
    }

    private const string CreatedPipeline = $"Pipeline.test-bulk-loader.Process.{LoaderHost.Created}";

    private static CloudEvent Traced(CloudEvent cloudEvent, out ActivityTraceId traceId)
    {
        cloudEvent[s_traceParent] = TraceRecorder.NewTraceParent(out traceId);
        return cloudEvent;
    }

    [Fact]
    public async Task EachMessageOfABatch_IsProcessedInItsProducersTrace_UnderOneSpan()
    {
        // A batch is processed the way each of its messages would be on its own: in the producer's
        // trace, one process span and one pipeline span per message, every step under it — the
        // steps after the batch send included.
        var events = new[] { Traced(Created("ORD-1"), out var t1), Traced(Created("ORD-2"), out var t2),
            Traced(Created("ORD-3"), out var t3) };
        var traces = new[] { t1, t2, t3 };
        using var spans = new TraceRecorder(traces);
        await using var host = await BulkLoaderHost.StartRoutingAsync();

        await host.DeliverBulkAsync(events);

        var pipelines = new List<Activity>();
        for (var i = 0; i < traces.Length; i++)
        {
            var trace = spans.InTrace(traces[i]);
            var process = Assert.Single(trace, s => s.Kind == ActivityKind.Consumer);
            Assert.Equal(events[i].Id, process.GetTagItem("messaging.message.id"));
            Assert.Equal(LoaderHost.Created, process.GetTagItem("intropy.loader.route"));
            var pipeline = Assert.Single(trace, s => s.DisplayName == CreatedPipeline);
            Assert.Equal(process.SpanId, pipeline.ParentSpanId);
            Assert.All(trace.Where(s => s != process && s != pipeline),
                s => Assert.Equal(pipeline.SpanId, s.ParentSpanId));
            Assert.Contains(trace, s => s.DisplayName == "Step.BulkSend");
            Assert.Contains(trace, s => s.DisplayName.StartsWith("Finalizer.", StringComparison.Ordinal));
            pipelines.Add(pipeline);
        }

        // The send itself is traced once, in a trace of its own, linked to every message.
        var batch = Assert.Single(spans.RootsLinkedTo(pipelines[0]));
        Assert.Equal("Step.BulkSend", batch.DisplayName);
        Assert.DoesNotContain(batch.TraceId, traces);
        Assert.All(pipelines, p => Assert.Contains(batch.Links, l => l.Context.SpanId == p.SpanId));
    }

    [Fact]
    public async Task AMessageThatCannotBePrepared_IsAnErrorInItsOwnTrace_AndOutOfTheSend()
    {
        var poisoned = Traced(Created("ORD-2"), out var poisonedTrace);
        poisoned[s_dataRef] = "../../outside/the/claim/store";
        var loaded = Traced(Created("ORD-1"), out var loadedTrace);
        using var spans = new TraceRecorder([poisonedTrace, loadedTrace]);
        await using var host = await BulkLoaderHost.StartRoutingAsync();

        await host.DeliverBulkAsync(loaded, poisoned);

        var trace = spans.InTrace(poisonedTrace);
        var process = Assert.Single(trace);
        Assert.Equal(ActivityKind.Consumer, process.Kind);
        Assert.Equal(ActivityStatusCode.Error, process.Status);
        var batch = Assert.Single(spans.RootsLinkedTo(
            spans.InTrace(loadedTrace).Single(s => s.DisplayName == CreatedPipeline)));
        Assert.Single(batch.Links);
    }

    [Fact]
    public async Task AnUnroutedBatch_IsRecordedOnEachMessagesOwnSpan()
    {
        var events = new[] { Traced(Created("ORD-1"), out var t1), Traced(Created("ORD-2"), out var t2) };
        using var spans = new TraceRecorder([t1, t2]);
        await using var host = await BulkLoaderHost.StartRoutingAsync();

        await host.Callback.DeliverBulkUnhandledAsync(events);

        foreach (var traceId in new[] { t1, t2 })
        {
            var process = Assert.Single(spans.InTrace(traceId));
            Assert.Equal("unrouted", process.GetTagItem("intropy.message.outcome"));
            Assert.Equal(ActivityStatusCode.Error, process.Status);
        }
    }

    [Fact]
    public async Task ASingleMessageLoader_LeavesABulkDeliveryForRedelivery()
    {
        // A loader registered for one message at a time, pushed a batch: its subscription declares
        // bulkSubscribe and the block was not registered for it. Nothing runs, nothing is lost.
        await using var host = await LoaderHost.StartRoutingAsync();

        var acks = await host.Callback!.DeliverBulkAsync([Created("ORD-1"), Created("ORD-2")]);

        Assert.All(acks, ack => Assert.Equal(DeliveryAck.Retry, ack));
        Assert.Empty(host.CreatedSender.Sent);
    }
}
