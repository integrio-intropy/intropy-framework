using System.Diagnostics;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.Logging.Abstractions;
using Status = Dapr.AppCallback.Autogen.Grpc.v1.TopicEventResponse.Types.TopicEventResponseStatus;

namespace Intropy.Framework.Hosting.Test.Messaging;

/// <summary>
/// What every subscribing block's consumer does with a pushed message: checks it is for its
/// subscription, runs the handler in the message's consumer span under the time limit, records the
/// metrics, and acknowledges only what was processed — never <c>DROP</c>.
/// </summary>
public class MessageConsumerTests
{
    private const string PubSub = "pubsub";

    private static MessageConsumer Consumer(MessageHandler handler, string topic = "orders",
        TimeSpan? maxProcessingTime = null, bool acknowledgeUnrouted = false, ActivityContext run = default) =>
        new(new MessageConsumerSettings(PubSub, topic, maxProcessingTime ?? TimeSpan.FromSeconds(30),
            TimeSpan.FromSeconds(5), acknowledgeUnrouted), handler, "test-component", NullLogger.Instance,
            run: run);

    private static MessageHandler Returns(MessageOutcome outcome, string? errorType = null, string? route = null) =>
        (_, _, _, _) => Task.FromResult(new HandledMessage(new PipelineOutcome(outcome, errorType, errorType), route));

    internal static TopicEventRequest Request(string id = "msg-1", string topic = "orders", string pubSub = PubSub,
        string type = "order.created", IDictionary<string, string>? extensions = null)
    {
        var request = new TopicEventRequest
        {
            Id = id, Source = "urn:test", Type = type, SpecVersion = "1.0", DataContentType = "application/json",
            Data = ByteString.CopyFromUtf8("{}"), PubsubName = pubSub, Topic = topic, Extensions = new Struct()
        };
        request.Extensions.Fields["subject"] = Value.ForString("order-42");
        foreach (var (key, value) in extensions ?? new Dictionary<string, string>())
            request.Extensions.Fields[key] = Value.ForString(value);
        return request;
    }

    [Theory]
    [InlineData(nameof(MessageOutcome.Processed), Status.Success)]
    [InlineData(nameof(MessageOutcome.Skipped), Status.Success)]
    [InlineData(nameof(MessageOutcome.Failed), Status.Retry)]
    [InlineData(nameof(MessageOutcome.Interrupted), Status.Retry)]
    [InlineData(nameof(MessageOutcome.Unrouted), Status.Retry)]
    public async Task HandleAsync_AcknowledgesOnlyWhatWasProcessed(string outcome, Status expected)
    {
        using var consumer = Consumer(Returns(System.Enum.Parse<MessageOutcome>(outcome)));

        var response = await consumer.HandleAsync(Request(), CancellationToken.None);

        Assert.Equal(expected, response.Status);
    }

    [Fact]
    public async Task HandleAsync_AcknowledgesAnUnroutedMessage_WhenTheConsumerDropsThem()
    {
        using var consumer = Consumer(Returns(MessageOutcome.Unrouted), acknowledgeUnrouted: true);

        var response = await consumer.HandleAsync(Request(), CancellationToken.None);

        Assert.Equal(Status.Success, response.Status);
    }

    [Fact]
    public async Task HandleAsync_LeavesTheMessageForRedelivery_WhenTheHandlerThrows()
    {
        using var consumer = Consumer((_, _, _, _) => throw new InvalidOperationException("Processing failed"));

        var response = await consumer.HandleAsync(Request(), CancellationToken.None);

        Assert.Equal(Status.Retry, response.Status);
    }

    [Theory]
    [InlineData("other-pubsub", "orders")]
    [InlineData(PubSub, "other-topic")]
    public async Task HandleAsync_LeavesADeliveryForAnotherSubscriptionForRedeliveryWithoutHandlingIt(string pubSub,
        string topic)
    {
        var handled = false;
        using var consumer = Consumer((_, _, _, _) =>
        {
            handled = true;
            return Task.FromResult(new HandledMessage(new PipelineOutcome(MessageOutcome.Processed)));
        });

        var response = await consumer.HandleAsync(Request(pubSub: pubSub, topic: topic), CancellationToken.None);

        Assert.Equal(Status.Retry, response.Status);
        Assert.False(handled);
    }

    [Fact]
    public async Task HandleAsync_CancelsAMessageExceedingItsTimeLimitAndLeavesItForRedelivery()
    {
        using var listener = Listen();
        var stopped = CaptureStopped(listener);
        using var consumer = Consumer(async (_, _, _, ct) =>
        {
            await Task.Delay(Timeout.Infinite, ct);
            return new HandledMessage(new PipelineOutcome(MessageOutcome.Processed));
        }, maxProcessingTime: TimeSpan.FromMilliseconds(100));

        var response = await consumer.HandleAsync(Request("msg-timeout"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(Status.Retry, response.Status);
        var span = Assert.Single(stopped(), a => (string?)a.GetTagItem("messaging.message.id") == "msg-timeout");
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal("timeout", span.GetTagItem("error.type"));
    }

    [Fact]
    public async Task HandleAsync_NamesTheSidecarAbandoningTheDelivery_AsCancelled()
    {
        using var listener = Listen();
        var stopped = CaptureStopped(listener);
        using var call = new CancellationTokenSource();
        using var consumer = Consumer(async (_, _, _, ct) =>
        {
            await call.CancelAsync();
            await Task.Delay(Timeout.Infinite, ct);
            return new HandledMessage(new PipelineOutcome(MessageOutcome.Processed));
        });

        var response = await consumer.HandleAsync(Request("msg-call-cancelled"), call.Token)
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(Status.Retry, response.Status);
        var span = Assert.Single(stopped(), a => (string?)a.GetTagItem("messaging.message.id") == "msg-call-cancelled");
        Assert.Equal("cancelled", span.GetTagItem("error.type"));
    }

    [Fact]
    public async Task HandleAsync_AfterStopBegan_LeavesTheMessageForRedeliveryWithoutHandlingIt()
    {
        var handled = false;
        using var consumer = Consumer((_, _, _, _) =>
        {
            handled = true;
            return Task.FromResult(new HandledMessage(new PipelineOutcome(MessageOutcome.Processed)));
        });
        await consumer.StopAsync();

        var response = await consumer.HandleAsync(Request(), CancellationToken.None);

        Assert.Equal(Status.Retry, response.Status);
        Assert.False(handled);
    }

    [Fact]
    public async Task HandleAsync_ContinuesThePropagatedTrace_AndDescribesTheSpanWithTheMessagingConventions()
    {
        var traceId = ActivityTraceId.CreateRandom();
        var parentSpanId = ActivitySpanId.CreateRandom();
        using var listener = Listen();
        Activity? span = null;
        using var consumer = Consumer((_, activity, _, _) =>
        {
            span = activity;
            return Task.FromResult(new HandledMessage(new PipelineOutcome(MessageOutcome.Processed), "order.created"));
        });

        await consumer.HandleAsync(Request("msg-7", extensions: new Dictionary<string, string>
        {
            ["traceparent"] = $"00-{traceId}-{parentSpanId}-01",
            ["retrycount"] = "2"
        }), CancellationToken.None);

        Assert.NotNull(span);
        Assert.Equal(traceId, span.TraceId);
        Assert.Equal(parentSpanId, span.ParentSpanId);
        Assert.Equal("process orders", span.DisplayName);
        Assert.Equal(ActivityKind.Consumer, span.Kind);
        Assert.Equal("dapr", span.GetTagItem("messaging.system"));
        Assert.Equal("process", span.GetTagItem("messaging.operation.type"));
        Assert.Equal("process", span.GetTagItem("messaging.operation.name"));
        Assert.Equal("orders", span.GetTagItem("messaging.destination.name"));
        Assert.Equal("msg-7", span.GetTagItem("messaging.message.id"));
        Assert.Equal(2, span.GetTagItem("messaging.message.body.size"));
        Assert.Equal(PubSub, span.GetTagItem("intropy.pubsub.name"));
        Assert.Equal("test-component", span.GetTagItem("intropy.component.name"));
        Assert.Equal("msg-7", span.GetTagItem("cloudevents.event_id"));
        Assert.Equal("urn:test", span.GetTagItem("cloudevents.event_source"));
        Assert.Equal("1.0", span.GetTagItem("cloudevents.event_spec_version"));
        Assert.Equal("order.created", span.GetTagItem("cloudevents.event_type"));
        Assert.Equal("order-42", span.GetTagItem("cloudevents.event_subject"));
        Assert.Equal("order.created", span.GetTagItem("intropy.loader.route"));
        Assert.Equal("processed", span.GetTagItem("intropy.message.outcome"));
        Assert.Equal(2L, span.GetTagItem("intropy.message.retry_count"));
        Assert.Equal(ActivityStatusCode.Unset, span.Status);
    }

    [Fact]
    public async Task HandleAsync_SetsTheMessagingAttributesAtStart_SoSamplersSeeThem()
    {
        var seen = new List<string>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Intropy.Framework.Hosting",
            Sample = (ref ActivityCreationOptions<ActivityContext> options) =>
            {
                if (options.Tags?.Any(t => t is { Key: "messaging.message.id", Value: "msg-sampled" }) == true)
                    lock (seen) seen.AddRange(options.Tags.Select(t => t.Key));
                return ActivitySamplingResult.AllDataAndRecorded;
            }
        };
        ActivitySource.AddActivityListener(listener);
        using var consumer = Consumer(Returns(MessageOutcome.Processed));

        await consumer.HandleAsync(Request("msg-sampled"), CancellationToken.None);

        Assert.Contains("messaging.system", seen);
        Assert.Contains("messaging.operation.name", seen);
        Assert.Contains("messaging.destination.name", seen);
        Assert.Contains("cloudevents.event_type", seen);
    }

    [Fact]
    public async Task HandleAsync_LinksTheSpanToTheSidecarsDelivery()
    {
        using var listener = Listen();
        Activity? span = null;
        using var consumer = Consumer((_, activity, _, _) =>
        {
            span = activity;
            return Task.FromResult(new HandledMessage(new PipelineOutcome(MessageOutcome.Processed)));
        });
        var delivery = new ActivityContext(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(),
            ActivityTraceFlags.Recorded, isRemote: true);

        await consumer.HandleAsync(Request(), CancellationToken.None, delivery);

        Assert.NotNull(span);
        var link = Assert.Single(span.Links);
        Assert.Equal(delivery.SpanId, link.Context.SpanId);
        Assert.Contains(link.Tags!, t => t is { Key: "intropy.link.kind", Value: "delivery" });
    }

    [Fact]
    public async Task HandleAsync_DoesNotMarkAnUnroutedMessageAsAnError_WhenTheConsumerDropsThem()
    {
        using var listener = Listen();
        var stopped = CaptureStopped(listener);
        using var metrics = new MetricCapture();
        var topic = $"unrouted-ack-{Guid.NewGuid()}";
        using var consumer = Consumer(Returns(MessageOutcome.Unrouted, "unrouted"), topic, acknowledgeUnrouted: true);

        await consumer.HandleAsync(Request("msg-unrouted-ack", topic: topic), CancellationToken.None);

        var span = Assert.Single(stopped(), a => (string?)a.GetTagItem("messaging.message.id") == "msg-unrouted-ack");
        Assert.Equal(ActivityStatusCode.Unset, span.Status);
        Assert.Null(span.GetTagItem("error.type"));
        Assert.Equal("unrouted", span.GetTagItem("intropy.message.outcome"));
        var consumed = Assert.Single(metrics.Of("messaging.client.consumed.messages", "messaging.destination.name", topic));
        Assert.False(consumed.Tags.ContainsKey("error.type"));
    }

    [Fact]
    public async Task HandleAsync_MarksAnUnroutedMessageAsAnError_WhenItIsLeftForTheDeadLetterQueue()
    {
        using var listener = Listen();
        var stopped = CaptureStopped(listener);
        using var consumer = Consumer(Returns(MessageOutcome.Unrouted, "unrouted"));

        await consumer.HandleAsync(Request("msg-unrouted-dlq"), CancellationToken.None);

        var span = Assert.Single(stopped(), a => (string?)a.GetTagItem("messaging.message.id") == "msg-unrouted-dlq");
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal("unrouted", span.GetTagItem("error.type"));
    }

    [Fact]
    public async Task HandleAsync_RecordsTheExceptionOnTheSpan_WhenTheHandlerReportsOne()
    {
        using var listener = Listen();
        var stopped = CaptureStopped(listener);
        var error = new InvalidOperationException("No service registered");
        using var consumer = Consumer((_, _, _, _) => Task.FromResult(new HandledMessage(
            PipelineOutcome.FromException(error, CancellationToken.None, CancellationToken.None), "a-route")));

        await consumer.HandleAsync(Request("msg-reported"), CancellationToken.None);

        var span = Assert.Single(stopped(), a => (string?)a.GetTagItem("messaging.message.id") == "msg-reported");
        Assert.Contains(span.Events, e => e.Name == "exception");
        Assert.Equal(typeof(InvalidOperationException).FullName, span.GetTagItem("error.type"));
    }

    [Fact]
    public async Task HandleAsync_TracesAndCountsADeliveryForAnotherSubscription_WithoutAProcessingDuration()
    {
        using var listener = Listen();
        var stopped = CaptureStopped(listener);
        using var metrics = new MetricCapture();
        var topic = $"unexpected-{Guid.NewGuid()}";
        using var consumer = Consumer(Returns(MessageOutcome.Processed));

        await consumer.HandleAsync(Request("msg-unexpected", topic: topic), CancellationToken.None);

        var span = Assert.Single(stopped(), a => (string?)a.GetTagItem("messaging.message.id") == "msg-unexpected");
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal("unexpected_subscription", span.GetTagItem("error.type"));
        var consumed = Assert.Single(metrics.Of("messaging.client.consumed.messages", "messaging.destination.name", topic));
        Assert.Equal("unexpected_subscription", consumed.Tags["error.type"]);
        Assert.Empty(metrics.Of("messaging.process.duration", "messaging.destination.name", topic));
    }

    [Fact]
    public async Task HandleAsync_ProcessesAnySubscriptionsDelivery_WhenItNamesNoTopic()
    {
        // The Subscription resource alone decides what is delivered: nothing to disagree with.
        var handled = 0;
        using var consumer = new MessageConsumer(new MessageConsumerSettings("", "", TimeSpan.FromSeconds(30),
                TimeSpan.FromSeconds(5)), (_, _, _, _) =>
            {
                handled++;
                return Task.FromResult(new HandledMessage(new PipelineOutcome(MessageOutcome.Processed)));
            }, "test-component", NullLogger.Instance);

        var response = await consumer.HandleAsync(Request(topic: "any-topic", pubSub: "any-pubsub"),
            CancellationToken.None);

        Assert.Equal(Status.Success, response.Status);
        Assert.Equal(1, handled);
    }

    [Fact]
    public async Task HandleAsync_AfterStopBegan_CountsTheMessageAsRejected_WithoutAProcessingDuration()
    {
        using var metrics = new MetricCapture();
        var topic = $"rejected-{Guid.NewGuid()}";
        using var consumer = Consumer(Returns(MessageOutcome.Processed), topic);
        await consumer.StopAsync();

        await consumer.HandleAsync(Request(topic: topic), CancellationToken.None);

        var consumed = Assert.Single(metrics.Of("messaging.client.consumed.messages", "messaging.destination.name", topic));
        Assert.Equal("rejected", consumed.Tags["intropy.message.outcome"]);
        Assert.Empty(metrics.Of("messaging.process.duration", "messaging.destination.name", topic));
    }

    [Fact]
    public async Task ActiveMessages_ReportsTheMessagesInFlight()
    {
        using var metrics = new MetricCapture();
        var topic = $"active-{Guid.NewGuid()}";
        var release = new TaskCompletionSource();
        var started = new TaskCompletionSource();
        using var consumer = Consumer(async (_, _, _, _) =>
        {
            started.SetResult();
            await release.Task;
            return new HandledMessage(new PipelineOutcome(MessageOutcome.Processed));
        }, topic);

        var handling = consumer.HandleAsync(Request(topic: topic), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        metrics.RecordObservableInstruments();
        release.SetResult();
        await handling;

        var active = Assert.Single(metrics.Of("intropy.messaging.active_messages", "messaging.destination.name", topic));
        Assert.Equal(1, active.Value);
        Assert.Equal("test-component", active.Tags["intropy.component.name"]);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("not-a-traceparent")]
    public async Task HandleAsync_StartsANewTrace_WhenTheMessageCarriesNoValidTraceContext(string? traceParent)
    {
        // Even when a span is current on the delivering thread, a message without trace context
        // is its own unit of work.
        using var jobSource = new ActivitySource($"consumer-test-{Guid.NewGuid()}");
        using var listener = Listen(jobSource);
        Activity? span = null;
        using var consumer = Consumer((_, activity, _, _) =>
        {
            span = activity;
            return Task.FromResult(new HandledMessage(new PipelineOutcome(MessageOutcome.Processed)));
        });
        var extensions = traceParent is null ? null : new Dictionary<string, string> { ["traceparent"] = traceParent };

        using var job = jobSource.StartActivity("job")!;
        await consumer.HandleAsync(Request(extensions: extensions), CancellationToken.None);

        Assert.NotNull(span);
        Assert.Equal(default, span.ParentSpanId);
        Assert.NotEqual(job.TraceId, span.TraceId);
    }

    [Fact]
    public async Task HandleAsync_LinksTheSpanToTheRunThatConsumedIt()
    {
        using var jobSource = new ActivitySource($"consumer-link-test-{Guid.NewGuid()}");
        using var listener = Listen(jobSource);
        using var job = jobSource.StartActivity("job")!;
        Activity? span = null;
        using var consumer = Consumer((_, activity, _, _) =>
        {
            span = activity;
            return Task.FromResult(new HandledMessage(new PipelineOutcome(MessageOutcome.Processed)));
        }, run: job.Context);

        await consumer.HandleAsync(Request(), CancellationToken.None);

        Assert.NotNull(span);
        Assert.Contains(span.Links, l => l.Context.SpanId == job.Context.SpanId);
    }

    [Fact]
    public async Task HandleAsync_MarksTheSpanAsAnError_WhenTheMessageFails()
    {
        using var listener = Listen();
        var stopped = CaptureStopped(listener);
        using var consumer = Consumer(Returns(MessageOutcome.Failed, "technical_failure"));

        await consumer.HandleAsync(Request("msg-err"), CancellationToken.None);

        var span = Assert.Single(stopped(), a => (string?)a.GetTagItem("messaging.message.id") == "msg-err");
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal("technical_failure", span.GetTagItem("error.type"));
    }

    [Fact]
    public async Task HandleAsync_RecordsTheExceptionOnTheSpan_WhenTheHandlerThrows()
    {
        using var listener = Listen();
        var stopped = CaptureStopped(listener);
        using var consumer = Consumer((_, _, _, _) => throw new InvalidOperationException("Processing failed"));

        await consumer.HandleAsync(Request("msg-throw"), CancellationToken.None);

        var span = Assert.Single(stopped(), a => (string?)a.GetTagItem("messaging.message.id") == "msg-throw");
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Contains(span.Events, e => e.Name == "exception");
        Assert.Equal(typeof(InvalidOperationException).FullName, span.GetTagItem("error.type"));
    }

    [Theory]
    [InlineData(nameof(MessageOutcome.Processed), "processed", null)]
    [InlineData(nameof(MessageOutcome.Skipped), "skipped", null)]
    [InlineData(nameof(MessageOutcome.Failed), "failed", "technical_failure")]
    [InlineData(nameof(MessageOutcome.Interrupted), "interrupted", null)]
    [InlineData(nameof(MessageOutcome.Unrouted), "unrouted", "unrouted")]
    public async Task HandleAsync_RecordsTheConsumedMessageAndItsProcessingDuration(string outcome,
        string outcomeName, string? errorType)
    {
        using var metrics = new MetricCapture();
        var topic = $"metrics-topic-{Guid.NewGuid()}";
        using var consumer = Consumer(Returns(System.Enum.Parse<MessageOutcome>(outcome), errorType, "a-route"), topic);

        await consumer.HandleAsync(Request(topic: topic), CancellationToken.None);

        var consumed = Assert.Single(metrics.Of("messaging.client.consumed.messages", "messaging.destination.name", topic));
        Assert.Equal(1, consumed.Value);
        Assert.Equal("dapr", consumed.Tags["messaging.system"]);
        Assert.Equal("process", consumed.Tags["messaging.operation.name"]);
        Assert.Equal(PubSub, consumed.Tags["intropy.pubsub.name"]);
        Assert.Equal(outcomeName, consumed.Tags["intropy.message.outcome"]);
        Assert.Equal("test-component", consumed.Tags["intropy.component.name"]);
        Assert.Equal("a-route", consumed.Tags["intropy.loader.route"]);
        Assert.Equal(errorType, consumed.Tags.GetValueOrDefault("error.type"));
        var duration = Assert.Single(metrics.Of("messaging.process.duration", "messaging.destination.name", topic));
        Assert.Equal(outcomeName, duration.Tags["intropy.message.outcome"]);
    }

    private static ActivityListener Listen(ActivitySource? testSource = null)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source == testSource || source.Name == "Intropy.Framework.Hosting",
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static Func<List<Activity>> CaptureStopped(ActivityListener listener)
    {
        var stopped = new List<Activity>();
        listener.ActivityStopped = activity => { lock (stopped) stopped.Add(activity); };
        return () => { lock (stopped) return [.. stopped]; };
    }
}
