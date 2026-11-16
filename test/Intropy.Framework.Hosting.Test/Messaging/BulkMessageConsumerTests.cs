using System.Diagnostics;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Status = Dapr.AppCallback.Autogen.Grpc.v1.TopicEventResponse.Types.TopicEventResponseStatus;

namespace Intropy.Framework.Hosting.Test.Messaging;

/// <summary>
/// What a bulk loader's consumer does with a pushed batch: checks it is for its subscription,
/// routes the batch as a whole, runs it through that route under the time limit, and answers with
/// one acknowledgement per entry — never <c>DROP</c>.
/// </summary>
public class BulkMessageConsumerTests
{
    private const string PubSub = "pubsub";
    private const string Topic = "orders";
    private const string EventType = "order.created";

    [Fact]
    public async Task HandleAsync_AnswersOneStatusPerEntry_InDeliveryOrder()
    {
        using var consumer = Consumer(Route(EventType, _ => MessageOutcome.Processed));

        var response = await consumer.HandleAsync(Batch("a", "b", "c"), CancellationToken.None);

        Assert.Equal(["a", "b", "c"], response.Statuses.Select(s => s.EntryId));
        Assert.All(response.Statuses, status => Assert.Equal(Status.Success, status.Status));
    }

    [Fact]
    public async Task HandleAsync_AcknowledgesEachEntryOnItsOwnOutcome()
    {
        // The batch's messages end differently: each entry is acknowledged on its own verdict, not
        // on the batch's worst.
        using var consumer = Consumer(Route(EventType, index => index switch
        {
            0 => MessageOutcome.Processed,
            1 => MessageOutcome.Failed,
            _ => MessageOutcome.Skipped
        }));

        var response = await consumer.HandleAsync(Batch("a", "b", "c"), CancellationToken.None);

        Assert.Equal([Status.Success, Status.Retry, Status.Success], response.Statuses.Select(s => s.Status));
    }

    [Fact]
    public async Task HandleAsync_LeavesTheWholeBatchForRedelivery_WhenItIsForAnotherSubscription()
    {
        using var consumer = Consumer(Route(EventType, _ => MessageOutcome.Processed));

        var response = await consumer.HandleAsync(BatchFrom("another-topic", "a", "b"), CancellationToken.None);

        Assert.Equal(["a", "b"], response.Statuses.Select(s => s.EntryId));
        Assert.All(response.Statuses, status => Assert.Equal(Status.Retry, status.Status));
    }

    [Fact]
    public async Task HandleAsync_LeavesTheWholeBatchForRedelivery_WhenItMixesEventTypes()
    {
        // The sidecar groups a batch by the route its rules selected, so a mixed batch means the
        // delivery broke that contract: none of it runs.
        using var consumer = Consumer(Route(EventType, _ => MessageOutcome.Processed));
        var batch = Batch("a", "b");
        batch.Entries[1].CloudEvent.Type = "order.cancelled";

        var response = await consumer.HandleAsync(batch, CancellationToken.None);

        Assert.All(response.Statuses, status => Assert.Equal(Status.Retry, status.Status));
    }

    [Fact]
    public async Task HandleAsync_LeavesAnUnroutedBatchForRedelivery_ByDefault()
    {
        using var consumer = Consumer(Route("order.shipped", _ => MessageOutcome.Processed));

        var response = await consumer.HandleAsync(Batch("a", "b"), CancellationToken.None);

        Assert.All(response.Statuses, status => Assert.Equal(Status.Retry, status.Status));
    }

    [Fact]
    public async Task HandleAsync_AcknowledgesAnUnroutedBatch_WhenTheConsumerDropsThem()
    {
        using var consumer = Consumer(Route("order.shipped", _ => MessageOutcome.Processed),
            acknowledgeUnrouted: true);

        var response = await consumer.HandleAsync(Batch("a", "b"), CancellationToken.None);

        Assert.All(response.Statuses, status => Assert.Equal(Status.Success, status.Status));
    }

    [Fact]
    public async Task HandleAsync_ReportsAnEntryDeliveredAsRawBytesOnItsOwn()
    {
        // A raw-bytes entry carries no CloudEvent type to route on; it is unrouted, and the rest of
        // the batch still runs.
        using var consumer = Consumer(Route(EventType, _ => MessageOutcome.Processed));
        var batch = Batch("a", "b");
        batch.Entries[0].Bytes = ByteString.CopyFromUtf8("{}");

        var response = await consumer.HandleAsync(batch, CancellationToken.None);

        Assert.Equal(Status.Retry, response.Statuses.Single(s => s.EntryId == "a").Status);
        Assert.Equal(Status.Success, response.Statuses.Single(s => s.EntryId == "b").Status);
    }

    [Fact]
    public async Task HandleAsync_AnswersNothingForAnEmptyBatch()
    {
        using var consumer = Consumer(Route(EventType, _ => MessageOutcome.Processed));

        var response = await consumer.HandleAsync(new TopicEventBulkRequest { PubsubName = PubSub, Topic = Topic },
            CancellationToken.None);

        Assert.Empty(response.Statuses);
    }

    [Fact]
    public async Task HandleAsync_LeavesTheBatchForRedelivery_WhenTheRouteThrows()
    {
        using var consumer = Consumer(new ThrowingRoute(EventType));

        var response = await consumer.HandleAsync(Batch("a", "b"), CancellationToken.None);

        Assert.All(response.Statuses, status => Assert.Equal(Status.Retry, status.Status));
    }

    [Fact]
    public async Task HandleAsync_LeavesTheBatchForRedelivery_WhenTheRouteAnswersTheWrongNumberOfOutcomes()
    {
        using var consumer = Consumer(new ShortCountRoute(EventType));

        var response = await consumer.HandleAsync(Batch("a", "b"), CancellationToken.None);

        Assert.All(response.Statuses, status => Assert.Equal(Status.Retry, status.Status));
    }

    [Fact]
    public async Task HandleAsync_LeavesTheBatchForRedelivery_WhenItRunsPastTheTimeLimit()
    {
        var route = new BlockingRoute(EventType);
        using var consumer = Consumer(route, maxProcessingTime: TimeSpan.FromMilliseconds(100));

        var response = await consumer.HandleAsync(Batch("a", "b"), CancellationToken.None);

        Assert.All(response.Statuses, status => Assert.Equal(Status.Retry, status.Status));
    }

    [Fact]
    public async Task HandleAsync_LeavesABatchDeliveredAfterStoppingForRedelivery_WithoutRunningIt()
    {
        var route = new BlockingRoute(EventType);
        using var consumer = Consumer(route);
        await consumer.StopAsync();

        var response = await consumer.HandleAsync(Batch("a", "b"), CancellationToken.None);

        Assert.All(response.Statuses, status => Assert.Equal(Status.Retry, status.Status));
        Assert.False(route.Entered.Task.IsCompleted);
    }

    [Fact]
    public async Task HandleAsync_CountsEachMessage_AndRecordsTheBatchDurationOnce()
    {
        // A batch has no per-message duration: its messages are counted, the batch is timed once,
        // and the per-message duration histogram is left alone.
        const string component = nameof(HandleAsync_CountsEachMessage_AndRecordsTheBatchDurationOnce);
        using var metrics = new MetricCapture();
        using var consumer = Consumer(Route(EventType, _ => MessageOutcome.Processed), componentName: component);

        await consumer.HandleAsync(Batch("a", "b", "c"), CancellationToken.None);

        Assert.Equal(3, metrics.Of("messaging.client.consumed.messages", "intropy.component.name", component)
            .Sum(m => m.Value));
        Assert.Single(metrics.Of("intropy.messaging.batch.process.duration", "intropy.component.name", component));
        Assert.Empty(metrics.Of("messaging.process.duration", "intropy.component.name", component));
    }

    [Fact]
    public async Task ActiveMessages_CountsTheMessagesOfABatchInFlight_NotTheBatch()
    {
        const string component = nameof(ActiveMessages_CountsTheMessagesOfABatchInFlight_NotTheBatch);
        using var metrics = new MetricCapture();
        var route = new BlockingRoute(EventType);
        using var consumer = Consumer(route, componentName: component);

        var handling = consumer.HandleAsync(Batch("a", "b", "c"), CancellationToken.None);
        await route.Entered.Task;
        metrics.RecordObservableInstruments();
        route.Release();
        await handling;

        var active = metrics.Of("intropy.messaging.active_messages", "intropy.component.name", component);
        Assert.Equal(3, active.Max(m => m.Value));
    }

    [Fact]
    public async Task HandleAsync_CountsAMixedBatchAsFailed()
    {
        // The guard paths still report what they did with the messages.
        const string component = nameof(HandleAsync_CountsAMixedBatchAsFailed);
        using var metrics = new MetricCapture();
        using var consumer = Consumer(Route(EventType, _ => MessageOutcome.Processed), componentName: component);
        var batch = Batch("a", "b");
        batch.Entries[1].CloudEvent.Type = "order.cancelled";

        await consumer.HandleAsync(batch, CancellationToken.None);

        var consumed = metrics.Of("messaging.client.consumed.messages", "intropy.component.name", component);
        Assert.Equal(2, consumed.Sum(m => m.Value));
        Assert.All(consumed, m => Assert.Equal("heterogeneous_batch", m.Tags["error.type"]));
    }

    private static BulkMessageConsumer Consumer(IBulkMessageRoute route, bool acknowledgeUnrouted = false,
        TimeSpan? maxProcessingTime = null, string componentName = "test-component") =>
        new(new MessageConsumerSettings(PubSub, Topic, maxProcessingTime ?? TimeSpan.FromSeconds(30),
                TimeSpan.FromSeconds(5), acknowledgeUnrouted), new BulkMessageRouteTable([route]),
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            componentName, NullLogger.Instance);

    /// <summary>A route that holds its batch until released or cancelled, then reports it
    /// processed.</summary>
    private sealed class BlockingRoute(string eventType) : IBulkMessageRoute
    {
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release() => _release.TrySetResult();

        public string Name => eventType;

        public bool IsCatchAll => false;

        public bool Matches(IncomingMessage message) =>
            string.Equals(message.Type, eventType, StringComparison.Ordinal);

        public void Verify(IServiceProvider scope, string componentName) { }

        public async Task<IReadOnlyList<PipelineOutcome>> ExecuteBatchAsync(IReadOnlyList<IncomingMessage> messages,
            IReadOnlyList<Activity?> spans, IServiceScopeFactory scopes, string componentName,
            CancellationToken interrupt, CancellationToken cancellationToken)
        {
            Entered.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken);
            return [.. messages.Select(_ => new PipelineOutcome(MessageOutcome.Processed, null, null))];
        }
    }

    private static FakeRoute Route(string eventType, Func<int, MessageOutcome> outcome) => new(eventType, outcome);

    private static TopicEventBulkRequest Batch(params string[] entryIds) => Build(entryIds, Topic);

    private static TopicEventBulkRequest BatchFrom(string topic, params string[] entryIds) => Build(entryIds, topic);

    private static TopicEventBulkRequest Build(string[] entryIds, string topic)
    {
        var batch = new TopicEventBulkRequest { Id = "batch-1", PubsubName = PubSub, Topic = topic };
        foreach (var entryId in entryIds)
        {
            var cloudEvent = new TopicEventCERequest
            {
                Id = $"msg-{entryId}", Source = "urn:test", Type = EventType, SpecVersion = "1.0",
                DataContentType = "application/json", Data = ByteString.CopyFromUtf8("{}"), Extensions = new Struct()
            };
            cloudEvent.Extensions.Fields["subject"] = Value.ForString($"order-{entryId}");
            batch.Entries.Add(new TopicEventBulkRequestEntry
            {
                EntryId = entryId, CloudEvent = cloudEvent, ContentType = "application/cloudevents+json"
            });
        }

        return batch;
    }

    /// <summary>A route that answers each message of a batch with the outcome its index maps to.</summary>
    private sealed class FakeRoute(string eventType, Func<int, MessageOutcome> outcome) : IBulkMessageRoute
    {
        public string Name => eventType;

        public bool IsCatchAll => false;

        public bool Matches(IncomingMessage message) =>
            string.Equals(message.Type, eventType, StringComparison.Ordinal);

        public void Verify(IServiceProvider scope, string componentName) { }

        public Task<IReadOnlyList<PipelineOutcome>> ExecuteBatchAsync(IReadOnlyList<IncomingMessage> messages,
            IReadOnlyList<Activity?> spans, IServiceScopeFactory scopes, string componentName,
            CancellationToken interrupt, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PipelineOutcome>>(
                [.. messages.Select((_, i) => new PipelineOutcome(outcome(i), null, null))]);
    }

    private sealed class ThrowingRoute(string eventType) : IBulkMessageRoute
    {
        public string Name => eventType;

        public bool IsCatchAll => false;

        public bool Matches(IncomingMessage message) =>
            string.Equals(message.Type, eventType, StringComparison.Ordinal);

        public void Verify(IServiceProvider scope, string componentName) { }

        public Task<IReadOnlyList<PipelineOutcome>> ExecuteBatchAsync(IReadOnlyList<IncomingMessage> messages,
            IReadOnlyList<Activity?> spans, IServiceScopeFactory scopes, string componentName,
            CancellationToken interrupt, CancellationToken cancellationToken) => throw new InvalidOperationException("the batch could not run");
    }

    /// <summary>A route that breaks its contract by answering fewer outcomes than it was given
    /// messages.</summary>
    private sealed class ShortCountRoute(string eventType) : IBulkMessageRoute
    {
        public string Name => eventType;

        public bool IsCatchAll => false;

        public bool Matches(IncomingMessage message) =>
            string.Equals(message.Type, eventType, StringComparison.Ordinal);

        public void Verify(IServiceProvider scope, string componentName) { }

        public Task<IReadOnlyList<PipelineOutcome>> ExecuteBatchAsync(IReadOnlyList<IncomingMessage> messages,
            IReadOnlyList<Activity?> spans, IServiceScopeFactory scopes, string componentName,
            CancellationToken interrupt, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PipelineOutcome>>(
                [new PipelineOutcome(MessageOutcome.Processed, null, null)]);
    }
}
