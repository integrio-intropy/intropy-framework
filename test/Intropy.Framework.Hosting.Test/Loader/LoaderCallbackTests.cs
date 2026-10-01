using System.Diagnostics;
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
/// The app-callback transport: the loader serves the Dapr gRPC app callback on its port, announces
/// no subscription (a declarative resource owns it), runs each pushed message through its routes
/// under its own time limit, and answers with the same acks as the streaming loader.
/// </summary>
[Collection(ProcessStateCollection.Name)]
public class LoaderCallbackTests
{
    private static readonly CloudEventAttribute s_traceParent =
        CloudEventAttribute.CreateExtension("traceparent", CloudEventAttributeType.String);

    private static OrderCreated Created(string orderId) => new(orderId, "CUST-1");

    /// <summary>A published event: its payload as JSON data, its subject the order.</summary>
    private static CloudEvent Event(string type, string subject, object data) => new(new[] { s_traceParent })
    {
        Id = Guid.NewGuid().ToString(),
        Source = new Uri("urn:test:source"),
        Type = type,
        Subject = subject,
        Time = DateTimeOffset.UtcNow,
        DataContentType = "application/json",
        Data = data
    };

    private static Task<LoaderHost> StartRoutingAsync(Action<LoaderOptions>? configure = null) =>
        LoaderHost.StartRoutingAsync(o =>
        {
            o.Transport = LoaderTransport.AppCallback;
            o.CallbackPort = AppCallbackDelivery.AvailablePort();
            configure?.Invoke(o);
        });

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
            await using var host = await LoaderHost.StartRoutingAsync(o => o.Transport = LoaderTransport.AppCallback);
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

    [Fact]
    public async Task AppCallback_RouteThatCannotBeComposed_StopsTheHostWithExitCode1BeforeServing()
    {
        var port = AppCallbackDelivery.AvailablePort();
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
                    o.Transport = LoaderTransport.AppCallback;
                    o.CallbackPort = port;
                }));

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

    [Fact]
    public async Task Routes_DispatchEachPushedMessageToItsEventTypesPipeline()
    {
        await using var host = await StartRoutingAsync();

        var createdAck = await host.Callback!.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        var cancelledAck = await host.Callback.DeliverAsync(Event(LoaderHost.Cancelled, "ORD-2",
            new OrderCancelled("ORD-2", "changed mind")));

        Assert.Equal(DeliveryAck.Success, createdAck);
        Assert.Equal(DeliveryAck.Success, cancelledAck);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
        Assert.Equal("ORD-2", Assert.Single(host.CancelledSender.Sent).Value.OrderId);
        Assert.Equal(["order.created:ORD-1", "order.cancelled:ORD-2"], host.Idempotency.Committed.Select(m => m.Id));
    }

    [Fact]
    public async Task Duplicate_IsAckedWithoutSending()
    {
        await using var host = await StartRoutingAsync();
        host.Idempotency.NextStatus = new StatusResponse(Action.Ignore, Reason.SameData);

        var ack = await host.Callback!.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Empty(host.CreatedSender.Sent);
    }

    [Fact]
    public async Task BusinessFailure_RoutesAnIncidentAndAcksSuccess()
    {
        await using var host = await LoaderHost.StartSingleAsync(order => order.OrderId == "ORD-BAD", o =>
        {
            o.Transport = LoaderTransport.AppCallback;
            o.CallbackPort = AppCallbackDelivery.AvailablePort();
        });

        var ack = await host.Callback!.DeliverAsync(Event("any.type", "ORD-BAD", Created("ORD-BAD")));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Single(host.Incidents.Incidents);
        Assert.Empty(host.CreatedSender.Sent);
    }

    [Fact]
    public async Task SenderFailure_LeavesTheMessageForRedelivery()
    {
        await using var host = await StartRoutingAsync();
        host.CreatedSender.Failure = new InvalidOperationException("Destination unreachable");

        var ack = await host.Callback!.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.Idempotency.Committed);
    }

    [Theory]
    [InlineData(UnroutedPolicy.DeadLetter, DeliveryAck.Drop)]
    [InlineData(UnroutedPolicy.Ack, DeliveryAck.Success)]
    [InlineData(UnroutedPolicy.Retry, DeliveryAck.Retry)]
    public async Task UnroutedMessage_IsAckedAsThePolicySays(UnroutedPolicy policy, DeliveryAck expected)
    {
        await using var host = await StartRoutingAsync(o => o.Unrouted = policy);

        var ack = await host.Callback!.DeliverAsync(Event("order.shipped", "ORD-1", Created("ORD-1")));

        Assert.Equal(expected, ack);
        Assert.Empty(host.CreatedSender.Sent);
        Assert.Empty(host.CancelledSender.Sent);
    }

    [Theory]
    [InlineData("other-pubsub", LoaderHost.Topic)]
    [InlineData(LoaderHost.PubSub, "other-topic")]
    public async Task DeliveryForAnotherSubscription_IsLeftForRedeliveryWithoutProcessing(string pubSub, string topic)
    {
        await using var host = await StartRoutingAsync();

        var ack = await host.Callback!.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")), pubSub,
            topic);

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.CreatedSender.Sent);
    }

    [Fact]
    public async Task MessageExceedingItsTimeLimit_IsCancelledAndLeftForRedelivery()
    {
        await using var host = await StartRoutingAsync(o => o.MaxMessageProcessingTime = TimeSpan.FromMilliseconds(200));
        // The sender never finishes on its own: only the loader's time limit ends it.
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var ack = await host.Callback!.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")))
            .WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.CreatedSender.Sent);
    }

    [Fact]
    public async Task Redelivery_ReachesThePipelineAsARetry()
    {
        await using var host = await StartRoutingAsync();

        await host.Callback!.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")), redelivery: true);

        Assert.True(Assert.Single(host.CreatedSender.Sent).IsRetry);
    }

    [Fact]
    public async Task PushedMessage_ContinuesThePublishersTraceAndCarriesItsRoute()
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
        await using var host = await StartRoutingAsync();
        var cloudEvent = Event(LoaderHost.Created, "ORD-1", Created("ORD-1"));
        cloudEvent[s_traceParent] = $"00-{traceId}-{ActivitySpanId.CreateRandom()}-01";

        await host.Callback!.DeliverAsync(cloudEvent);

        Activity activity;
        lock (processed) activity = Assert.Single(processed);
        Assert.Equal($"process {LoaderHost.Topic}", activity.DisplayName);
        Assert.Equal(LoaderHost.Created, activity.GetTagItem("intropy.route"));
        Assert.Equal(cloudEvent.Id, activity.GetTagItem("messaging.message.id"));
    }

    [Fact]
    public async Task Shutdown_LetsTheMessageInFlightFinishBeforeTheServerStops()
    {
        var host = await StartRoutingAsync();
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var delivery = host.Callback!.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
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
        var host = await StartRoutingAsync(o => o.Unrouted = UnroutedPolicy.Ack);
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inFlight = host.Callback!.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        await host.CreatedSender.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The message in flight holds the server open while it drains.
        var stopping = host.Host.StopAsync();
        var probe = DeliveryAck.Success;
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (probe == DeliveryAck.Success && DateTime.UtcNow < deadline)
            probe = await host.Callback.DeliverAsync(Event("order.shipped", "ORD-0", Created("ORD-0")));
        var late = await host.Callback.DeliverAsync(Event(LoaderHost.Cancelled, "ORD-2",
            new OrderCancelled("ORD-2", "changed mind")));
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
        var host = await StartRoutingAsync(o => o.ShutdownGracePeriod = TimeSpan.FromMilliseconds(200));
        // The sender never finishes on its own: only the interruption ends it.
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivery = host.Callback!.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        await host.CreatedSender.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await host.Host.StopAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(DeliveryAck.Retry, await delivery.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Empty(host.CreatedSender.Sent);
        var outcomes = metrics.Of("messaging.client.consumed.messages", "intropy.route", LoaderHost.Created)
            .Select(m => m.Tags["intropy.message.outcome"]);
        Assert.Equal(["interrupted"], outcomes);
        await host.DisposeAsync();
    }
}
