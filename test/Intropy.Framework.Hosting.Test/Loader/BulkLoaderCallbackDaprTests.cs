using System.Diagnostics;
using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// The bulk loader against a real Dapr sidecar (daprd 1.18, RabbitMQ) over the gRPC app callback:
/// one subscription with <c>bulkSubscribe</c> on and a rule per event type, deployed as
/// <see cref="LoaderCallbackDaprTests"/> deploys the single-message loader. What the in-process
/// harness only models is pinned here: that the sidecar calls the stable bulk callback, that its
/// entries carry the CloudEvent the loader routes and traces by, that it never mixes types in one
/// call, and that it honours each entry's own status.
/// </summary>
[Trait("Category", "Integration")]
public class BulkLoaderCallbackDaprTests(BulkLoaderDaprFixture dapr) : IClassFixture<BulkLoaderDaprFixture>
{
    private static readonly TimeSpan s_timeout = TimeSpan.FromSeconds(30);

    private RecordingBatchSender<OrderCreated> Created => dapr.Loader.CreatedSender;
    private RecordingBatchSender<OrderCancelled> Cancelled => dapr.Loader.CancelledSender;

    private static string NewOrderId() => $"ORD-{Guid.NewGuid():N}";

    [Fact]
    public async Task PublishedMessages_ArriveInBatches_AndAreEachSentOnce()
    {
        var orderIds = Enumerable.Range(0, 10).Select(_ => NewOrderId()).ToList();
        var deadLettered = await dapr.DeadLetteredAsync();

        foreach (var orderId in orderIds)
            await PublishCreatedAsync(orderId);
        await WaitUntilAsync(() => orderIds.All(id => SentTo(id) > 0));
        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.All(orderIds, id => Assert.Equal(1, SentTo(id)));
        Assert.Contains(Created.Batches, batch => batch.Count(o => orderIds.Contains(o.OrderId)) > 1);
        Assert.Equal(deadLettered, await dapr.DeadLetteredAsync());
    }

    [Fact]
    public async Task InterleavedTypes_ArriveAsSeparateBatches_EachOnItsOwnRoute()
    {
        // One subscription carries both types; the sidecar groups each pull by matched rule, so the
        // loader is never handed a mixed batch — it would leave one for redelivery, not load it.
        var created = Enumerable.Range(0, 4).Select(_ => NewOrderId()).ToList();
        var cancelled = Enumerable.Range(0, 4).Select(_ => NewOrderId()).ToList();
        var deadLettered = await dapr.DeadLetteredAsync();

        foreach (var (create, cancel) in created.Zip(cancelled))
        {
            await PublishCreatedAsync(create);
            await dapr.PublishAsync(Guid.NewGuid().ToString(), LoaderHost.Cancelled, cancel,
                new OrderCancelled(cancel, "changed my mind"));
        }
        await WaitUntilAsync(() => created.All(id => SentTo(id) > 0) &&
                                   cancelled.All(id => Cancelled.Batches.Any(b => b.Any(o => o.OrderId == id))));
        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.All(created, id => Assert.Equal(1, SentTo(id)));
        Assert.All(cancelled, id => Assert.Equal(1, Cancelled.Batches.Sum(b => b.Count(o => o.OrderId == id))));
        Assert.Equal(deadLettered, await dapr.DeadLetteredAsync());
    }

    [Fact]
    public async Task AMessageThatCannotBePrepared_IsDeadLetteredAlone_AndTheRestAreSent()
    {
        // A claim pointer this framework never mints fails that entry alone: the sidecar retries
        // only it and hands it to the broker's dead-letter queue, while the batch's other entries
        // are sent once and acknowledged.
        var loaded = new[] { NewOrderId(), NewOrderId() };
        var poisoned = NewOrderId();
        var deadLettered = await dapr.DeadLetteredAsync();

        await PublishCreatedAsync(loaded[0]);
        await PublishCreatedAsync(poisoned, new Dictionary<string, string>
        {
            [CloudEventExtensionKeys.DataRef] = "../../outside/the/claim/store"
        });
        await PublishCreatedAsync(loaded[1]);
        await WaitUntilAsync(async () => await dapr.DeadLetteredAsync() > deadLettered);
        await WaitUntilAsync(() => loaded.All(id => SentTo(id) > 0));
        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.All(loaded, id => Assert.Equal(1, SentTo(id)));
        Assert.Equal(0, SentTo(poisoned));
        Assert.Equal(deadLettered + 1, await dapr.DeadLetteredAsync());
    }

    [Fact]
    public async Task AFailedBatch_IsRedeliveredByTheSidecar_AndSentOnce()
    {
        var orderIds = Enumerable.Range(0, 3).Select(_ => NewOrderId()).ToList();
        var deadLettered = await dapr.DeadLetteredAsync();
        Created.FailNext(1);
        try
        {
            foreach (var orderId in orderIds)
                await PublishCreatedAsync(orderId);
            await WaitUntilAsync(() => orderIds.All(id => SentTo(id) > 0));
            await Task.Delay(TimeSpan.FromSeconds(2));

            Assert.All(orderIds, id => Assert.Equal(1, SentTo(id)));
            Assert.Equal(deadLettered, await dapr.DeadLetteredAsync());
        }
        finally
        {
            Created.FailNext(0);
        }
    }

    [Fact]
    public async Task AnUnroutedType_EndsInTheBrokersDeadLetterQueue()
    {
        var orderId = NewOrderId();
        var deadLettered = await dapr.DeadLetteredAsync();

        await dapr.PublishAsync(Guid.NewGuid().ToString(), "order.shipped", orderId, new OrderCreated(orderId, "CUST-1"));
        await WaitUntilAsync(async () => await dapr.DeadLetteredAsync() > deadLettered);

        Assert.Equal(0, SentTo(orderId));
    }

    [Fact]
    public async Task EachMessageOfABatch_IsProcessedInItsPublishersTrace_AndTheSendLinksThemAll()
    {
        // The sidecar carries each message's trace context to the loader on the message itself:
        // each is processed in its publisher's trace, under one pipeline span, and the one send that
        // carried them is linked from — and to — every one of them.
        var traceParents = new[] { TraceRecorder.NewTraceParent(out var t1), TraceRecorder.NewTraceParent(out var t2) };
        var traces = new[] { t1, t2 };
        using var spans = new TraceRecorder(traces);

        for (var i = 0; i < traces.Length; i++)
        {
            var orderId = NewOrderId();
            await dapr.PublishAsync(Guid.NewGuid().ToString(), LoaderHost.Created, orderId,
                new OrderCreated(orderId, "CUST-1"), traceParent: traceParents[i]);
        }

        await WaitUntilAsync(() => traces.All(t => spans.InTrace(t).Any(s => s.Kind == ActivityKind.Consumer)));
        foreach (var traceId in traces)
        {
            var trace = spans.InTrace(traceId);
            var process = Assert.Single(trace, s => s.Kind == ActivityKind.Consumer);
            var pipeline = Assert.Single(trace, s => s.DisplayName.StartsWith("Pipeline.", StringComparison.Ordinal));
            Assert.Equal(process.SpanId, pipeline.ParentSpanId);
            var send = Assert.Single(trace, s => s.DisplayName == "Step.BulkSend");
            Assert.Equal(pipeline.SpanId, send.ParentSpanId);
            var batch = Assert.Single(spans.RootsLinkedTo(pipeline));
            Assert.Equal(batch.SpanId, Assert.Single(send.Links).Context.SpanId);
        }
    }

    private Task PublishCreatedAsync(string orderId, IReadOnlyDictionary<string, string>? extensions = null) =>
        dapr.PublishAsync(Guid.NewGuid().ToString(), LoaderHost.Created, orderId, new OrderCreated(orderId, "CUST-1"),
            extensions: extensions);

    private int SentTo(string orderId) => Created.Batches.Sum(batch => batch.Count(o => o.OrderId == orderId));

    private static Task WaitUntilAsync(Func<bool> condition) => WaitUntilAsync(() => Task.FromResult(condition()));

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + s_timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"The condition was not met within {s_timeout}.");
            await Task.Delay(100);
        }
    }
}
