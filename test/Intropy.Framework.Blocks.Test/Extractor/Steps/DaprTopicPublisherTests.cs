using System.Diagnostics;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using Dapr.Client;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Intropy.Framework.Blocks.Test.Extractor.Steps;

/// <summary>
/// The extractor's pub/sub publisher: the event goes to the configured topic under a producer span
/// whose trace context the event carries, so the loader can continue the extractor's trace.
/// </summary>
public class DaprTopicPublisherTests
{
    private static readonly Uri Source = new("urn:example:orders");

    [Fact]
    public async Task ExecuteAsync_PublishesUnderAProducerSpan_WhoseContextTheEventCarries()
    {
        var daprClient = Substitute.For<DaprClient>();
        (string Topic, byte[] Data)? published = null;
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(ci => published = (ci.ArgAt<string>(1), ci.ArgAt<ReadOnlyMemory<byte>>(2).ToArray()));
        var publisher = new DaprTopicPublisher<Context>(daprClient, "pubsub", "orders.accepted", Source, "orders.accepted");
        var input = Event();

        var (root, spans) = await CaptureBlocksSpansAsync(async () =>
        {
            var (result, _) = await publisher.ExecuteAsync(input, new Context(new Dictionary<string, string>()),
                CancellationToken.None);
            Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(result);
        });

        var send = Assert.Single(spans);
        Assert.Equal("send orders.accepted", send.DisplayName);
        Assert.Equal(ActivityKind.Producer, send.Kind);
        Assert.Equal(root.SpanId, send.ParentSpanId);
        Assert.Equal("dapr", send.GetTagItem("messaging.system"));
        Assert.Equal("send", send.GetTagItem("messaging.operation.type"));
        Assert.Equal("orders.accepted", send.GetTagItem("messaging.destination.name"));
        Assert.Equal(input.Id, send.GetTagItem("messaging.message.id"));
        Assert.NotNull(published);
        Assert.Equal("orders.accepted", published.Value.Topic);
        var traceParent = CloudEventAttribute.CreateExtension("traceparent", CloudEventAttributeType.String);
        var envelope = new JsonEventFormatter().DecodeStructuredModeMessage(published.Value.Data, null, [traceParent]);
        Assert.Equal(send.Id, envelope[traceParent]);
    }

    [Fact]
    public async Task ExecuteAsync_PublishesAnObjectPayload_AsACamelCaseJsonObject()
    {
        // Sidecar rules (CEL on event.data) and non-.NET consumers read the payload as JSON is
        // usually written.
        var daprClient = Substitute.For<DaprClient>();
        byte[]? published = null;
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(ci => published = ci.ArgAt<ReadOnlyMemory<byte>>(2).ToArray());
        var publisher = new DaprTopicPublisher<Context>(daprClient, "pubsub", "orders.accepted", Source, "orders.accepted");
        var input = Event();
        input.Data = new { OrderId = "ORD-1", CustomerId = "CUST-1" };

        await publisher.ExecuteAsync(input, new Context(new Dictionary<string, string>()), CancellationToken.None);

        using var envelope = System.Text.Json.JsonDocument.Parse(published!);
        var data = envelope.RootElement.GetProperty("data");
        Assert.Equal(System.Text.Json.JsonValueKind.Object, data.ValueKind);
        Assert.Equal("ORD-1", data.GetProperty("orderId").GetString());
        Assert.Equal("CUST-1", data.GetProperty("customerId").GetString());
    }

    [Fact]
    public async Task ExecuteAsync_CarriesTheCurrentTraceContext_WhenTheProducerSpanIsNotSampled()
    {
        // Nobody listens to the Blocks source: the event still continues whatever trace is current
        var daprClient = Substitute.For<DaprClient>();
        byte[]? published = null;
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(ci => published = ci.ArgAt<ReadOnlyMemory<byte>>(2).ToArray());
        var publisher = new DaprTopicPublisher<Context>(daprClient, "pubsub", "orders.accepted", Source, "orders.accepted");
        using var testSource = new ActivitySource($"publisher-test-{Guid.NewGuid()}");
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source == testSource,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        using (var current = testSource.StartActivity("step")!)
        {
            await publisher.ExecuteAsync(Event(), new Context(new Dictionary<string, string>()), CancellationToken.None);

            var traceParent = CloudEventAttribute.CreateExtension("traceparent", CloudEventAttributeType.String);
            var envelope = new JsonEventFormatter().DecodeStructuredModeMessage(published!, null, [traceParent]);
            Assert.Equal(current.Id, envelope[traceParent]);
        }
    }

    [Fact]
    public async Task ExecuteAsync_AppliesTheConfiguredType_WhenTheSerializeStepDidNotSetOne()
    {
        var daprClient = Substitute.For<DaprClient>();
        byte[]? published = null;
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(ci => published = ci.ArgAt<ReadOnlyMemory<byte>>(2).ToArray());
        var publisher = new DaprTopicPublisher<Context>(daprClient, "pubsub", "orders.accepted", Source, "orders.accepted");

        await publisher.ExecuteAsync(Event(), new Context(new Dictionary<string, string>()), CancellationToken.None);

        var envelope = new JsonEventFormatter().DecodeStructuredModeMessage(published!, null, []);
        Assert.Equal("orders.accepted", envelope.Type);
    }

    [Fact]
    public async Task ExecuteAsync_KeepsTheTypeSetByTheSerializeStep_WhenPresent()
    {
        // A component-supplied type extractor wins over the configured fallback.
        var daprClient = Substitute.For<DaprClient>();
        byte[]? published = null;
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(ci => published = ci.ArgAt<ReadOnlyMemory<byte>>(2).ToArray());
        var publisher = new DaprTopicPublisher<Context>(daprClient, "pubsub", "orders.accepted", Source, "orders.accepted");
        var input = Event();
        input.Type = "io.intropy.orders.new";

        await publisher.ExecuteAsync(input, new Context(new Dictionary<string, string>()), CancellationToken.None);

        var envelope = new JsonEventFormatter().DecodeStructuredModeMessage(published!, null, []);
        Assert.Equal("io.intropy.orders.new", envelope.Type);
    }

    [Fact]
    public async Task ExecuteAsync_MarksTheSendSpanAsAnError_WhenPublishingFails()
    {
        var daprClient = Substitute.For<DaprClient>();
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("broker unavailable"));
        var publisher = new DaprTopicPublisher<Context>(daprClient, "pubsub", "orders.accepted", Source, "orders.accepted");

        var (_, spans) = await CaptureBlocksSpansAsync(() => Assert.ThrowsAsync<InvalidOperationException>(() =>
            publisher.ExecuteAsync(Event(), new Context(new Dictionary<string, string>()), CancellationToken.None)));

        var send = Assert.Single(spans);
        Assert.Equal(ActivityStatusCode.Error, send.Status);
        Assert.Equal(typeof(InvalidOperationException).FullName, send.GetTagItem("error.type"));
        Assert.Contains(send.Events, e => e.Name == "exception");
    }

    private static CloudEvent Event() => new()
    {
        Id = Guid.NewGuid().ToString(),
        Subject = "order-42",
        Time = DateTimeOffset.UtcNow,
        DataContentType = "application/json",
        Data = "{}"
    };

    /// <summary>Runs <paramref name="act"/> under a test-owned root span and returns it with the
    /// Blocks spans in its trace, ignoring spans from tests running in parallel.</summary>
    private static async Task<(Activity Root, List<Activity> Spans)> CaptureBlocksSpansAsync(Func<Task> act)
    {
        using var testSource = new ActivitySource($"publisher-test-{Guid.NewGuid()}");
        var stopped = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source == testSource || source.Name == "Intropy.Framework.Blocks",
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (stopped) stopped.Add(activity); }
        };
        ActivitySource.AddActivityListener(listener);

        Activity root;
        using (root = testSource.StartActivity("test")!)
            await act();

        lock (stopped)
            return (root, stopped.Where(a => a.Source.Name == "Intropy.Framework.Blocks" && a.TraceId == root.TraceId).ToList());
    }
}
