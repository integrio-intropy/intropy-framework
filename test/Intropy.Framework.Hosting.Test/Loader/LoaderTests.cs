using System.Diagnostics;
using System.Text.Json;
using CloudNative.CloudEvents;
using Grpc.Core;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Testing.Delivery;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// The loader's consumption contract, through a real generic host and its app callback: messages
/// are routed by their CloudEvent type, each route's pipeline result becomes the message's ack,
/// unrouted messages follow the unrouted policy, and shutdown drains the messages in flight. The
/// callback itself (port, foreign deliveries, time limit) is in <see cref="LoaderCallbackTests"/>.
/// </summary>
[Collection(ProcessStateCollection.Name)]
public class LoaderTests
{
    private static readonly CloudEventAttribute s_traceParent =
        CloudEventAttribute.CreateExtension("traceparent", CloudEventAttributeType.String);

    private static OrderCreated Created(string orderId) => new(orderId, "CUST-1");

    private static OrderCancelled Cancelled(string orderId) => new(orderId, "changed mind");

    /// <summary>A published event: its payload as JSON data, its subject the order.</summary>
    private static CloudEvent Event(string type, string subject, object data) => new([s_traceParent])
    {
        Id = Guid.NewGuid().ToString(),
        Source = new Uri("urn:test:source"),
        Type = type,
        Subject = subject,
        Time = DateTimeOffset.UtcNow,
        DataContentType = "application/json",
        Data = data
    };

    [Fact]
    public async Task SingleRoute_LoadsTheMessageAndAcksSuccess()
    {
        await using var host = await LoaderHost.StartSingleAsync();

        var ack = await host.DeliverAsync(Event("any.type", "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
    }

    [Fact]
    public async Task SingleRoute_KeysIdempotencyOnTheSubject()
    {
        await using var host = await LoaderHost.StartSingleAsync();

        await host.DeliverAsync(Event("any.type", "ORD-1", Created("ORD-1")));

        Assert.Equal("ORD-1", Assert.Single(host.Idempotency.Committed).Id);
    }

    [Fact]
    public async Task Routes_DispatchEachMessageToItsEventTypesPipeline()
    {
        await using var host = await LoaderHost.StartRoutingAsync();

        var createdAck = await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        var cancelledAck = await host.DeliverAsync(Event(LoaderHost.Cancelled, "ORD-2", Cancelled("ORD-2")));

        Assert.Equal(DeliveryAck.Success, createdAck);
        Assert.Equal(DeliveryAck.Success, cancelledAck);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
        Assert.Equal("ORD-2", Assert.Single(host.CancelledSender.Sent).Value.OrderId);
    }

    [Fact]
    public async Task Routes_KeepIdempotencyRecordsApartPerEventType()
    {
        await using var host = await LoaderHost.StartRoutingAsync();

        await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        await host.DeliverAsync(Event(LoaderHost.Cancelled, "ORD-1", Cancelled("ORD-1")));

        Assert.Equal(["order.created:ORD-1", "order.cancelled:ORD-1"], host.Idempotency.Committed.Select(m => m.Id));
    }

    [Theory]
    [InlineData(UnroutedPolicy.DeadLetter, DeliveryAck.Retry)]
    [InlineData(UnroutedPolicy.Ack, DeliveryAck.Success)]
    public async Task UnroutedMessage_IsAckedAsThePolicySays(UnroutedPolicy policy, DeliveryAck expected)
    {
        await using var host = await LoaderHost.StartRoutingAsync(o => o.Unrouted = policy);

        var ack = await host.DeliverAsync(Event("order.shipped", "ORD-1", Created("ORD-1")));

        Assert.Equal(expected, ack);
        Assert.Empty(host.CreatedSender.Sent);
        Assert.Empty(host.CancelledSender.Sent);
    }

    [Theory]
    [InlineData(UnroutedPolicy.DeadLetter, DeliveryAck.Retry)]
    [InlineData(UnroutedPolicy.Ack, DeliveryAck.Success)]
    public async Task MessageOnTheUnhandledRoute_IsUnrouted_EvenWhenARouteHandlesItsType(UnroutedPolicy policy,
        DeliveryAck expected)
    {
        // A content filter on the subscription left it out: the sidecar's rules decide, not the type.
        await using var host = await LoaderHost.StartRoutingAsync(o => o.Unrouted = policy);

        var ack = await host.DeliverUnhandledAsync(Event(LoaderHost.Cancelled, "ORD-1", Cancelled("ORD-1")));

        Assert.Equal(expected, ack);
        Assert.Empty(host.CancelledSender.Sent);
    }

    [Fact]
    public async Task Loader_NeverAnswersDrop()
    {
        // Without a Dapr dead-letter topic the sidecar discards a dropped message: whatever the
        // loader cannot load must go back to the broker, which dead-letters it.
        await using var host = await LoaderHost.StartRoutingAsync();
        host.CancelledSender.Failure = new InvalidOperationException("Destination unreachable");

        var acks = new[]
        {
            await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1"))),
            await host.DeliverAsync(Event("order.shipped", "ORD-2", Created("ORD-2"))),
            await host.DeliverAsync(Event(LoaderHost.Cancelled, "ORD-3", Cancelled("ORD-3"))),
            await host.DeliverAsync(Event(LoaderHost.Created, "ORD-4", "not an order"))
        };

        Assert.DoesNotContain(DeliveryAck.Drop, acks);
    }

    [Fact]
    public void On_WithAnEventTypeAlreadyRouted_Throws()
    {
        var routes = new LoaderRoutes().On<OrderCreated, OrderCreated, Context>(LoaderHost.Created, (b, _) => b,
            (metadata, isRetry) => new Context(metadata, isRetry));

        Assert.Throws<ArgumentException>(() => routes.On<OrderCreated, OrderCreated, Context>(LoaderHost.Created,
            (b, _) => b, (metadata, isRetry) => new Context(metadata, isRetry)));
    }

    [Fact]
    public async Task Duplicate_IsAckedWithoutSending()
    {
        await using var host = await LoaderHost.StartRoutingAsync();
        host.Idempotency.NextStatus = new StatusResponse(Action.Ignore, Reason.SameData);

        var ack = await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Empty(host.CreatedSender.Sent);
        Assert.Empty(host.Idempotency.Committed);
    }

    [Fact]
    public async Task BusinessFailure_RoutesAnIncidentAndAcksSuccess()
    {
        await using var host = await LoaderHost.StartSingleAsync(reject: order => order.OrderId == "ORD-BAD");

        var ack = await host.DeliverAsync(Event("any.type", "ORD-BAD", Created("ORD-BAD")));

        // Redelivering a rejected message would loop forever.
        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Single(host.Incidents.Incidents);
        Assert.Empty(host.CreatedSender.Sent);
    }

    [Fact]
    public async Task SenderFailure_LeavesTheMessageForRedelivery()
    {
        await using var host = await LoaderHost.StartRoutingAsync();
        host.CreatedSender.Failure = new InvalidOperationException("Destination unreachable");

        var ack = await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.Idempotency.Committed);
    }

    [Fact]
    public async Task Redelivery_ReachesThePipelineAsARetry()
    {
        await using var host = await LoaderHost.StartRoutingAsync();

        await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")), redelivery: true);

        Assert.True(Assert.Single(host.CreatedSender.Sent).IsRetry);
    }

    [Fact]
    public async Task PayloadPublishedAsAJsonString_IsUnwrapped()
    {
        // A publisher that sends pre-serialized JSON text puts a JSON string inside the CloudEvent.
        await using var host = await LoaderHost.StartRoutingAsync();

        var ack = await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1",
            JsonSerializer.Serialize(Created("ORD-1"))));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
    }

    [Fact]
    public async Task Message_ContinuesThePublishersTraceAndCarriesItsRoute()
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
        await using var host = await LoaderHost.StartRoutingAsync();
        var cloudEvent = Event(LoaderHost.Created, "ORD-1", Created("ORD-1"));
        cloudEvent[s_traceParent] = $"00-{traceId}-{ActivitySpanId.CreateRandom()}-01";

        await host.DeliverAsync(cloudEvent);

        Activity activity;
        lock (processed) activity = Assert.Single(processed);
        Assert.Equal($"process {LoaderHost.Topic}", activity.DisplayName);
        Assert.Equal(LoaderHost.Created, activity.GetTagItem("intropy.route"));
        Assert.Equal(cloudEvent.Id, activity.GetTagItem("messaging.message.id"));
    }

    [Fact]
    public async Task Shutdown_LetsTheMessageInFlightFinish()
    {
        var host = await LoaderHost.StartRoutingAsync();
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var delivery = host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        await host.CreatedSender.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var stopping = host.Host.StopAsync();
        host.CreatedSender.Gate.SetResult();

        Assert.Equal(DeliveryAck.Success, await delivery.WaitAsync(TimeSpan.FromSeconds(10)));
        await stopping;
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
        await host.DisposeAsync();
    }

    [Fact]
    public async Task Shutdown_LeavesMessagesDeliveredAfterStopBeganForRedelivery()
    {
        // Unrouted messages are acked without side effects, so they can probe whether stop has begun.
        var host = await LoaderHost.StartRoutingAsync(o => o.Unrouted = UnroutedPolicy.Ack);
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inFlight = host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        await host.CreatedSender.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The message in flight holds the callback open while it drains.
        var stopping = host.Host.StopAsync();
        var probe = DeliveryAck.Success;
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (probe == DeliveryAck.Success && DateTime.UtcNow < deadline)
            probe = await host.DeliverAsync(Event("order.shipped", "ORD-0", Created("ORD-0")));
        var late = await host.DeliverAsync(Event(LoaderHost.Cancelled, "ORD-2", Cancelled("ORD-2")));
        host.CreatedSender.Gate.SetResult();

        Assert.Equal(DeliveryAck.Retry, probe);
        Assert.Equal(DeliveryAck.Retry, late);
        Assert.Empty(host.CancelledSender.Sent);
        Assert.Equal(DeliveryAck.Success, await inFlight.WaitAsync(TimeSpan.FromSeconds(10)));
        await stopping;
        await host.DisposeAsync();
    }

    [Fact]
    public async Task Shutdown_InterruptsAMessageOutlivingTheGracePeriodWithoutCountingItAsFailed()
    {
        using var metrics = new MetricCapture();
        var host = await LoaderHost.StartRoutingAsync(o => o.ShutdownGracePeriod = TimeSpan.FromMilliseconds(200));
        // The sender never finishes on its own: only the interruption ends it.
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivery = host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        await host.CreatedSender.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await host.Host.StopAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(DeliveryAck.Retry, await delivery.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Empty(host.CreatedSender.Sent);
        var outcomes = metrics.Of("messaging.client.consumed.messages", "intropy.route", LoaderHost.Created)
            .Select(m => m.Tags["intropy.message.outcome"]);
        Assert.Equal(["interrupted"], outcomes);
        await host.DisposeAsync();
    }

    [Fact]
    public async Task RouteThatCannotBeComposed_StopsTheHostWithExitCode1BeforeConsuming()
    {
        try
        {
            // The route has no sender: its pipeline cannot be built.
            await using var host = await LoaderHost.StartAsync((services, _) => services.AddLoader<OrderCreated, OrderCreated, Context>(
                (b, _) => b.WithDeserializer(new JsonDeserializer<OrderCreated>()),
                (metadata, isRetry) => new Context(metadata, isRetry),
                LoaderHost.Defaults));

            var lifetime = host.Host.Services.GetRequiredService<IHostApplicationLifetime>();
            await Task.Delay(Timeout.Infinite, lifetime.ApplicationStopping).ContinueWith(_ => { },
                TaskScheduler.Default).WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(1, Environment.ExitCode);
            var refused = await Assert.ThrowsAsync<RpcException>(() => host.Callback!.GetSubscriptionsAsync());
            Assert.Equal(StatusCode.Unavailable, refused.StatusCode);
        }
        finally
        {
            Environment.ExitCode = 0;
        }
    }
}
