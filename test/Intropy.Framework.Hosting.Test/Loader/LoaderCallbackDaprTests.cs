using System.Diagnostics;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// The loader against a real Dapr sidecar (daprd 1.18, RabbitMQ), deployed as a
/// production loader is: a declarative <c>Subscription</c> resource without a Dapr dead-letter
/// topic, the broker's own dead-letter queue, and a <c>Resiliency</c> policy that retries inbound
/// deliveries. A message the loader leaves for redelivery is retried by the sidecar, then handed
/// back to the broker, which dead-letters it.
/// </summary>
[Trait("Category", "Integration")]
public class LoaderCallbackDaprTests(LoaderDaprFixture dapr) : IClassFixture<LoaderDaprFixture>
{
    private static readonly TimeSpan s_timeout = TimeSpan.FromSeconds(20);

    private RecordingSender<OrderCreated> Sender => dapr.Loader.CreatedSender;

    private static string NewOrderId() => $"ORD-{Guid.NewGuid():N}";

    [Fact]
    public async Task PublishedMessage_IsDeliveredOnceAndAcknowledged()
    {
        var orderId = NewOrderId();
        var attempts = Sender.Attempts;
        var deadLettered = await dapr.DeadLetteredAsync();

        await dapr.PublishAsync(Guid.NewGuid().ToString(), LoaderHost.Created, orderId, new OrderCreated(orderId, "CUST-1"));
        await WaitUntilAsync(() => SentTo(orderId) > 0);
        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.Equal(1, SentTo(orderId));
        Assert.Equal(attempts + 1, Sender.Attempts);
        Assert.Equal(deadLettered, await dapr.DeadLetteredAsync());
    }

    [Fact]
    public async Task Retry_IsRedeliveredByTheSidecarsRetryPolicyWithoutARetryMarker()
    {
        var orderId = NewOrderId();
        var attempts = Sender.Attempts;
        var deadLettered = await dapr.DeadLetteredAsync();
        Sender.FailNext(1);

        await dapr.PublishAsync(Guid.NewGuid().ToString(), LoaderHost.Created, orderId, new OrderCreated(orderId, "CUST-1"));
        await WaitUntilAsync(() => SentTo(orderId) > 0);

        Assert.Equal(attempts + 2, Sender.Attempts);
        // The sidecar's retry carries nothing that marks it as one: the pipeline cannot tell.
        Assert.False(Sender.Sent.Single(s => s.Value.OrderId == orderId).IsRetry);
        Assert.Equal(deadLettered, await dapr.DeadLetteredAsync());
    }

    [Fact]
    public async Task RetriesRunningOut_HandTheMessageToTheBrokersDeadLetterQueue()
    {
        var orderId = NewOrderId();
        var attempts = Sender.Attempts;
        var deadLettered = await dapr.DeadLetteredAsync();
        Sender.FailNext(LoaderDaprFixture.SidecarRetries + 10);
        try
        {
            await dapr.PublishAsync(Guid.NewGuid().ToString(), LoaderHost.Created, orderId, new OrderCreated(orderId, "CUST-1"));
            await WaitUntilAsync(async () => await dapr.DeadLetteredAsync() > deadLettered);

            Assert.Equal(0, SentTo(orderId));
            Assert.Equal(attempts + 1 + LoaderDaprFixture.SidecarRetries, Sender.Attempts);
        }
        finally
        {
            Sender.FailNext(0);
        }
    }

    [Fact]
    public async Task UnroutedMessage_EndsInTheBrokersDeadLetterQueue()
    {
        var orderId = NewOrderId();
        var deadLettered = await dapr.DeadLetteredAsync();

        await dapr.PublishAsync(Guid.NewGuid().ToString(), "order.shipped", orderId, new OrderCreated(orderId, "CUST-1"));
        await WaitUntilAsync(async () => await dapr.DeadLetteredAsync() > deadLettered);

        Assert.Equal(0, SentTo(orderId));
    }

    [Fact]
    public async Task ContentFilter_SelectsTheMessagesItsRuleMatches_AndLeavesTheRestUnrouted()
    {
        // The subscription's rule for cancellations filters on the payload (event.data.reason): a
        // cancellation it selects reaches its route; one it leaves out arrives on the default route,
        // is unrouted although a route handles its type, and ends in the broker's dead-letter queue.
        var cancelled = dapr.Loader.CancelledSender;
        var selected = NewOrderId();
        var filtered = NewOrderId();
        var deadLettered = await dapr.DeadLetteredAsync();

        await dapr.PublishAsync(Guid.NewGuid().ToString(), LoaderHost.Cancelled, selected,
            new OrderCancelled(selected, LoaderDaprFixture.HandledCancellationReason));
        await dapr.PublishAsync(Guid.NewGuid().ToString(), LoaderHost.Cancelled, filtered,
            new OrderCancelled(filtered, "fraud-review"));
        await WaitUntilAsync(() => cancelled.Sent.Any(s => s.Value.OrderId == selected));
        await WaitUntilAsync(async () => await dapr.DeadLetteredAsync() > deadLettered);

        Assert.DoesNotContain(cancelled.Sent, s => s.Value.OrderId == filtered);
    }

    [Fact]
    public async Task PublishedMessage_ContinuesThePublishersTrace()
    {
        var traceId = ActivityTraceId.CreateRandom();
        var processed = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Intropy.Framework.Hosting",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.TraceId == traceId && activity.Kind == ActivityKind.Consumer)
                    lock (processed) processed.Add(activity);
            }
        };
        ActivitySource.AddActivityListener(listener);
        var orderId = NewOrderId();

        // The sidecar takes the trace context from the publish request, not from the CloudEvent.
        await dapr.PublishAsync(Guid.NewGuid().ToString(), LoaderHost.Created, orderId, new OrderCreated(orderId, "CUST-1"),
            traceParent: $"00-{traceId}-{ActivitySpanId.CreateRandom()}-01");
        await WaitUntilAsync(() => { lock (processed) return processed.Count > 0; });

        Activity activity;
        lock (processed) activity = Assert.Single(processed);
        Assert.Equal(LoaderHost.Created, activity.GetTagItem("intropy.route"));
    }

    private int SentTo(string orderId) => Sender.Sent.Count(s => s.Value.OrderId == orderId);

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

/// <summary>
/// A loader deployed without a <c>Resiliency</c> policy: the sidecar does not retry a message the
/// loader answered <c>RETRY</c> — it hands it straight back to the broker, which dead-letters it.
/// (A streaming subscription behaved the same, verified by a probe on 2026-10-01.)
/// </summary>
[Trait("Category", "Integration")]
public class LoaderCallbackDaprWithoutRetryPolicyTests(LoaderDaprFixtureWithoutRetryPolicy dapr)
    : IClassFixture<LoaderDaprFixtureWithoutRetryPolicy>
{
    [Fact]
    public async Task Retry_IsDeadLetteredAtOnce()
    {
        var sender = dapr.Loader.CreatedSender;
        var orderId = $"ORD-{Guid.NewGuid():N}";
        var attempts = sender.Attempts;
        var deadLettered = await dapr.DeadLetteredAsync();
        sender.FailNext(1);

        await dapr.PublishAsync(Guid.NewGuid().ToString(), LoaderHost.Created, orderId, new OrderCreated(orderId, "CUST-1"));
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (await dapr.DeadLetteredAsync() <= deadLettered)
        {
            Assert.True(DateTime.UtcNow < deadline, "The message was not dead-lettered within 20 seconds.");
            await Task.Delay(100);
        }
        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.Equal(attempts + 1, sender.Attempts);
        Assert.DoesNotContain(sender.Sent, s => s.Value.OrderId == orderId);
    }
}
