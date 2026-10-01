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
/// The loader's consumption contract, through a real generic host and for every transport: messages
/// are routed by their CloudEvent type, each route's pipeline result becomes the message's ack,
/// unrouted messages follow the unrouted policy, and shutdown drains the messages in flight.
/// What only one transport has lives in <see cref="LoaderStreamingTests"/> and
/// <see cref="LoaderCallbackTests"/>.
/// </summary>
[Collection(ProcessStateCollection.Name)]
public class LoaderTests
{
    private static readonly CloudEventAttribute s_traceParent =
        CloudEventAttribute.CreateExtension("traceparent", CloudEventAttributeType.String);

    public static TheoryData<LoaderTransport> Transports => [LoaderTransport.Streaming, LoaderTransport.AppCallback];

    public static TheoryData<LoaderTransport, UnroutedPolicy, DeliveryAck> UnroutedAcks
    {
        get
        {
            var data = new TheoryData<LoaderTransport, UnroutedPolicy, DeliveryAck>();
            foreach (var transport in Enum.GetValues<LoaderTransport>())
            {
                data.Add(transport, UnroutedPolicy.DeadLetter, DeliveryAck.Drop);
                data.Add(transport, UnroutedPolicy.Ack, DeliveryAck.Success);
                data.Add(transport, UnroutedPolicy.Retry, DeliveryAck.Retry);
            }

            return data;
        }
    }

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

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task SingleRoute_LoadsTheMessageAndAcksSuccess(LoaderTransport transport)
    {
        await using var host = await LoaderHost.StartSingleAsync(transport: transport);

        var ack = await host.DeliverAsync(Event("any.type", "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task SingleRoute_KeysIdempotencyOnTheSubject(LoaderTransport transport)
    {
        await using var host = await LoaderHost.StartSingleAsync(transport: transport);

        await host.DeliverAsync(Event("any.type", "ORD-1", Created("ORD-1")));

        Assert.Equal("ORD-1", Assert.Single(host.Idempotency.Committed).Id);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task Routes_DispatchEachMessageToItsEventTypesPipeline(LoaderTransport transport)
    {
        await using var host = await LoaderHost.StartRoutingAsync(transport: transport);

        var createdAck = await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        var cancelledAck = await host.DeliverAsync(Event(LoaderHost.Cancelled, "ORD-2", Cancelled("ORD-2")));

        Assert.Equal(DeliveryAck.Success, createdAck);
        Assert.Equal(DeliveryAck.Success, cancelledAck);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
        Assert.Equal("ORD-2", Assert.Single(host.CancelledSender.Sent).Value.OrderId);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task Routes_KeepIdempotencyRecordsApartPerEventType(LoaderTransport transport)
    {
        await using var host = await LoaderHost.StartRoutingAsync(transport: transport);

        await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        await host.DeliverAsync(Event(LoaderHost.Cancelled, "ORD-1", Cancelled("ORD-1")));

        Assert.Equal(["order.created:ORD-1", "order.cancelled:ORD-1"], host.Idempotency.Committed.Select(m => m.Id));
    }

    [Theory]
    [MemberData(nameof(UnroutedAcks))]
    public async Task UnroutedMessage_IsAckedAsThePolicySays(LoaderTransport transport, UnroutedPolicy policy,
        DeliveryAck expected)
    {
        await using var host = await LoaderHost.StartRoutingAsync(o => o.Unrouted = policy, transport: transport);

        var ack = await host.DeliverAsync(Event("order.shipped", "ORD-1", Created("ORD-1")));

        Assert.Equal(expected, ack);
        Assert.Empty(host.CreatedSender.Sent);
        Assert.Empty(host.CancelledSender.Sent);
    }

    [Fact]
    public void AddLoader_WhenRoutingDeadLettersWithoutADeadLetterTopic_Throws()
    {
        var services = new ServiceCollection();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddLoader(
            o =>
            {
                o.PubSubName = LoaderHost.PubSub;
                o.TopicName = LoaderHost.Topic;
            },
            routes => routes.On<OrderCreated, OrderCreated, Context>(LoaderHost.Created, (b, _) => b,
                (metadata, isRetry) => new Context(metadata, isRetry))));

        Assert.Contains(nameof(LoaderOptions.DeadLetterTopic), error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void On_WithAnEventTypeAlreadyRouted_Throws()
    {
        var routes = new LoaderRoutes().On<OrderCreated, OrderCreated, Context>(LoaderHost.Created, (b, _) => b,
            (metadata, isRetry) => new Context(metadata, isRetry));

        Assert.Throws<ArgumentException>(() => routes.On<OrderCreated, OrderCreated, Context>(LoaderHost.Created,
            (b, _) => b, (metadata, isRetry) => new Context(metadata, isRetry)));
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task Duplicate_IsAckedWithoutSending(LoaderTransport transport)
    {
        await using var host = await LoaderHost.StartRoutingAsync(transport: transport);
        host.Idempotency.NextStatus = new StatusResponse(Action.Ignore, Reason.SameData);

        var ack = await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Empty(host.CreatedSender.Sent);
        Assert.Empty(host.Idempotency.Committed);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task BusinessFailure_RoutesAnIncidentAndAcksSuccess(LoaderTransport transport)
    {
        await using var host = await LoaderHost.StartSingleAsync(reject: order => order.OrderId == "ORD-BAD",
            transport: transport);

        var ack = await host.DeliverAsync(Event("any.type", "ORD-BAD", Created("ORD-BAD")));

        // Redelivering a rejected message would loop forever.
        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Single(host.Incidents.Incidents);
        Assert.Empty(host.CreatedSender.Sent);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task SenderFailure_LeavesTheMessageForRedelivery(LoaderTransport transport)
    {
        await using var host = await LoaderHost.StartRoutingAsync(transport: transport);
        host.CreatedSender.Failure = new InvalidOperationException("Destination unreachable");

        var ack = await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.Idempotency.Committed);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task Redelivery_ReachesThePipelineAsARetry(LoaderTransport transport)
    {
        await using var host = await LoaderHost.StartRoutingAsync(transport: transport);

        await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")), redelivery: true);

        Assert.True(Assert.Single(host.CreatedSender.Sent).IsRetry);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task PayloadPublishedAsAJsonString_IsUnwrapped(LoaderTransport transport)
    {
        // The framework's extractor publishes its payload as a JSON string inside the CloudEvent.
        await using var host = await LoaderHost.StartRoutingAsync(transport: transport);

        var ack = await host.DeliverAsync(Event(LoaderHost.Created, "ORD-1",
            JsonSerializer.Serialize(Created("ORD-1"))));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task Message_ContinuesThePublishersTraceAndCarriesItsRoute(LoaderTransport transport)
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
        await using var host = await LoaderHost.StartRoutingAsync(transport: transport);
        var cloudEvent = Event(LoaderHost.Created, "ORD-1", Created("ORD-1"));
        cloudEvent[s_traceParent] = $"00-{traceId}-{ActivitySpanId.CreateRandom()}-01";

        await host.DeliverAsync(cloudEvent);

        Activity activity;
        lock (processed) activity = Assert.Single(processed);
        Assert.Equal($"process {LoaderHost.Topic}", activity.DisplayName);
        Assert.Equal(LoaderHost.Created, activity.GetTagItem("intropy.route"));
        Assert.Equal(cloudEvent.Id, activity.GetTagItem("messaging.message.id"));
    }

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task Shutdown_LetsTheMessageInFlightFinish(LoaderTransport transport)
    {
        var host = await LoaderHost.StartRoutingAsync(transport: transport);
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

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task Shutdown_LeavesMessagesDeliveredAfterStopBeganForRedelivery(LoaderTransport transport)
    {
        // Unrouted messages are acked without side effects, so they can probe whether stop has begun.
        var host = await LoaderHost.StartRoutingAsync(o => o.Unrouted = UnroutedPolicy.Ack, transport: transport);
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inFlight = host.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        await host.CreatedSender.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The message in flight holds the transport open while it drains.
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

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task Shutdown_InterruptsAMessageOutlivingTheGracePeriodWithoutCountingItAsFailed(
        LoaderTransport transport)
    {
        using var metrics = new MetricCapture();
        var host = await LoaderHost.StartRoutingAsync(o => o.ShutdownGracePeriod = TimeSpan.FromMilliseconds(200),
            transport: transport);
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

    [Theory]
    [MemberData(nameof(Transports))]
    public async Task RouteThatCannotBeComposed_StopsTheHostWithExitCode1BeforeConsuming(LoaderTransport transport)
    {
        try
        {
            // The route has no sender: its pipeline cannot be built.
            await using var host = await LoaderHost.StartAsync((services, _) => services.AddLoader<OrderCreated, OrderCreated, Context>(
                (b, _) => b.WithDeserializer(new JsonDeserializer<OrderCreated>()),
                (metadata, isRetry) => new Context(metadata, isRetry),
                o =>
                {
                    o.PubSubName = LoaderHost.PubSub;
                    o.TopicName = LoaderHost.Topic;
                    o.Transport = transport;
                    if (transport == LoaderTransport.AppCallback)
                        o.CallbackPort = AppCallbackDelivery.AvailablePort();
                }), waitForSubscription: false);

            var lifetime = host.Host.Services.GetRequiredService<IHostApplicationLifetime>();
            await Task.Delay(Timeout.Infinite, lifetime.ApplicationStopping).ContinueWith(_ => { },
                TaskScheduler.Default).WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(1, Environment.ExitCode);
            Assert.Empty(host.Subscriber.Subscriptions);
            if (host.Callback is not null)
            {
                var refused = await Assert.ThrowsAsync<RpcException>(() => host.Callback.GetSubscriptionsAsync());
                Assert.Equal(StatusCode.Unavailable, refused.StatusCode);
            }
        }
        finally
        {
            Environment.ExitCode = 0;
        }
    }
}
