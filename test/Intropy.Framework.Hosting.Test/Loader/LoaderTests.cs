using System.Text.Json;
using CloudNative.CloudEvents;
using Dapr.Messaging.PublishSubscribe;
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
/// The loader's consumption contract, through a real generic host: messages are routed by their
/// CloudEvent type, each route's pipeline result becomes the message's ack, unrouted messages follow
/// the unrouted policy, and the subscription survives a broken stream and drains on shutdown.
/// </summary>
[Collection(ProcessStateCollection.Name)]
public class LoaderTests
{
    private static OrderCreated Created(string orderId) => new(orderId, "CUST-1");

    private static OrderCancelled Cancelled(string orderId) => new(orderId, "changed mind");

    /// <summary>A published event: its payload as JSON data, its subject the order.</summary>
    private static CloudEvent Event(string type, string subject, object data) => new()
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

        var ack = await host.Subscriber.DeliverAsync(Event("any.type", "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
    }

    [Fact]
    public async Task SingleRoute_KeysIdempotencyOnTheSubject()
    {
        await using var host = await LoaderHost.StartSingleAsync();

        await host.Subscriber.DeliverAsync(Event("any.type", "ORD-1", Created("ORD-1")));

        Assert.Equal("ORD-1", Assert.Single(host.Idempotency.Committed).Id);
    }

    [Fact]
    public async Task Routes_DispatchEachMessageToItsEventTypesPipeline()
    {
        await using var host = await LoaderHost.StartRoutingAsync();

        var createdAck = await host.Subscriber.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        var cancelledAck = await host.Subscriber.DeliverAsync(Event(LoaderHost.Cancelled, "ORD-2", Cancelled("ORD-2")));

        Assert.Equal(DeliveryAck.Success, createdAck);
        Assert.Equal(DeliveryAck.Success, cancelledAck);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
        Assert.Equal("ORD-2", Assert.Single(host.CancelledSender.Sent).Value.OrderId);
    }

    [Fact]
    public async Task Routes_KeepIdempotencyRecordsApartPerEventType()
    {
        await using var host = await LoaderHost.StartRoutingAsync();

        await host.Subscriber.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        await host.Subscriber.DeliverAsync(Event(LoaderHost.Cancelled, "ORD-1", Cancelled("ORD-1")));

        Assert.Equal(["order.created:ORD-1", "order.cancelled:ORD-1"], host.Idempotency.Committed.Select(m => m.Id));
    }

    [Theory]
    [InlineData(UnroutedPolicy.DeadLetter, DeliveryAck.Drop)]
    [InlineData(UnroutedPolicy.Ack, DeliveryAck.Success)]
    [InlineData(UnroutedPolicy.Retry, DeliveryAck.Retry)]
    public async Task UnroutedMessage_IsAckedAsThePolicySays(UnroutedPolicy policy, DeliveryAck expected)
    {
        await using var host = await LoaderHost.StartRoutingAsync(o => o.Unrouted = policy);

        var ack = await host.Subscriber.DeliverAsync(Event("order.shipped", "ORD-1", Created("ORD-1")));

        Assert.Equal(expected, ack);
        Assert.Empty(host.CreatedSender.Sent);
        Assert.Empty(host.CancelledSender.Sent);
    }

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

    [Fact]
    public async Task Duplicate_IsAckedWithoutSending()
    {
        await using var host = await LoaderHost.StartRoutingAsync();
        host.Idempotency.NextStatus = new StatusResponse(Action.Ignore, Reason.SameData);

        var ack = await host.Subscriber.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Empty(host.CreatedSender.Sent);
        Assert.Empty(host.Idempotency.Committed);
    }

    [Fact]
    public async Task BusinessFailure_RoutesAnIncidentAndAcksSuccess()
    {
        await using var host = await LoaderHost.StartSingleAsync(reject: order => order.OrderId == "ORD-BAD");

        var ack = await host.Subscriber.DeliverAsync(Event("any.type", "ORD-BAD", Created("ORD-BAD")));

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

        var ack = await host.Subscriber.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.Idempotency.Committed);
    }

    [Fact]
    public async Task Redelivery_ReachesThePipelineAsARetry()
    {
        await using var host = await LoaderHost.StartRoutingAsync();

        await host.Subscriber.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")), redelivery: true);

        Assert.True(Assert.Single(host.CreatedSender.Sent).IsRetry);
    }

    [Fact]
    public async Task PayloadPublishedAsAJsonString_IsUnwrapped()
    {
        // The framework's extractor publishes its payload as a JSON string inside the CloudEvent.
        await using var host = await LoaderHost.StartRoutingAsync();

        var ack = await host.Subscriber.DeliverAsync(Event(LoaderHost.Created, "ORD-1",
            JsonSerializer.Serialize(Created("ORD-1"))));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Equal("ORD-1", Assert.Single(host.CreatedSender.Sent).Value.OrderId);
    }

    [Fact]
    public async Task BrokenStream_IsReopened()
    {
        await using var host = await LoaderHost.StartRoutingAsync();
        var first = host.Subscriber.Current;

        first.Break();
        await host.Subscriber.WaitForSubscriptionAsync(2);

        Assert.True(first.Disposed);
        var ack = await host.Subscriber.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
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
    public async Task Shutdown_LetsTheMessageInFlightFinishBeforeClosingTheStream()
    {
        var host = await LoaderHost.StartRoutingAsync();
        host.CreatedSender.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var delivery = host.Subscriber.DeliverAsync(Event(LoaderHost.Created, "ORD-1", Created("ORD-1")));
        await host.CreatedSender.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var stopping = host.Host.StopAsync();
        host.CreatedSender.Gate.SetResult();

        Assert.Equal(DeliveryAck.Success, await delivery);
        await stopping;
        Assert.True(host.Subscriber.Current.Disposed);
        host.Host.Dispose();
    }

    [Fact]
    public async Task RouteThatCannotBeComposed_StopsTheHostWithExitCode1()
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
                }), waitForSubscription: false);

            var lifetime = host.Host.Services.GetRequiredService<IHostApplicationLifetime>();
            await Task.Delay(Timeout.Infinite, lifetime.ApplicationStopping).ContinueWith(_ => { },
                TaskScheduler.Default).WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(1, Environment.ExitCode);
            Assert.Empty(host.Subscriber.Subscriptions);
        }
        finally
        {
            Environment.ExitCode = 0;
        }
    }
}
