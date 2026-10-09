using System.Diagnostics;
using System.Text.Json;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Receive.Steps;

/// <summary>
/// The built-in receive-side publisher: the item goes to the configured topic as the structured
/// envelope the base step builds, so the send side gets the context metadata back.
/// </summary>
public class DaprTopicEnqueuerTests
{
    private static readonly FrameworkOptions Options = new() { ComponentName = "orders", ServiceNamespace = "example" };

    [Fact]
    public async Task ExecuteAsync_PublishesTheStructuredEnvelopeToTheConfiguredTopic()
    {
        var daprClient = Substitute.For<DaprClient>();
        (string PubSub, string Topic, byte[] Data, string? ContentType)? published = null;
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(ci => published = (ci.ArgAt<string>(0), ci.ArgAt<string>(1),
                ci.ArgAt<ReadOnlyMemory<byte>>(2).ToArray(), ci.ArgAt<string>(3)));
        var enqueuer = new DaprTopicEnqueuer<Context>(daprClient, "internal-orders", "hop", Options);
        var item = new SourceItem("order-1.json", "{}"u8.ToArray());
        var context = new Context(new Dictionary<string, string> { [SourceContextKeys.FileName] = "order-1.json" });

        var (result, _) = await enqueuer.ExecuteAsync(item, context, CancellationToken.None);

        Assert.IsType<TechnicalStepResult<SourceItem>.Success>(result);
        Assert.NotNull(published);
        Assert.Equal("internal-orders", published.Value.PubSub);
        Assert.Equal("hop", published.Value.Topic);
        Assert.Equal("application/cloudevents+json", published.Value.ContentType);
        var metadataAttribute = CloudEventAttribute.CreateExtension("metadata", CloudEventAttributeType.String);
        var envelope = new JsonEventFormatter().DecodeStructuredModeMessage(published.Value.Data, null, [metadataAttribute]);
        Assert.Equal("{}"u8.ToArray(), (byte[])envelope.Data!);
        var metadata = JsonSerializer.Deserialize<Dictionary<string, string>>((string)envelope[metadataAttribute]!);
        Assert.Equal("order-1.json", metadata![SourceContextKeys.FileName]);
    }

    [Fact]
    public async Task PublishProbeAsync_PublishesAProbeEnvelopeToTheSameTopic()
    {
        // The probe must take the files' path, or its round trip proves nothing about them
        var daprClient = Substitute.For<DaprClient>();
        (string PubSub, string Topic, byte[] Data, string? ContentType)? published = null;
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(ci => published = (ci.ArgAt<string>(0), ci.ArgAt<string>(1),
                ci.ArgAt<ReadOnlyMemory<byte>>(2).ToArray(), ci.ArgAt<string>(3)));
        var probe = new DaprTopicEnqueuer<Context>(daprClient, "internal-orders", "hop", Options);

        await probe.PublishProbeAsync("probe-1", CancellationToken.None);

        Assert.NotNull(published);
        Assert.Equal(("internal-orders", "hop", "application/cloudevents+json"),
            (published.Value.PubSub, published.Value.Topic, published.Value.ContentType));
        var envelope = new JsonEventFormatter().DecodeStructuredModeMessage(published.Value.Data, null, null);
        Assert.Equal("probe-1", envelope.Id);
        Assert.Equal(InternalQueueMessageTypes.Probe, envelope.Type);
        Assert.Equal(new Uri("urn:$orders"), envelope.Source);
        Assert.Null(envelope.Data);
    }

    [Fact]
    public async Task ExecuteAsync_PublishesUnderAProducerSpan_WhoseContextTheEnvelopeCarries()
    {
        // The consumer continues the send span, following the OpenTelemetry messaging conventions
        var daprClient = Substitute.For<DaprClient>();
        byte[]? published = null;
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(ci => published = ci.ArgAt<ReadOnlyMemory<byte>>(2).ToArray());
        var enqueuer = new DaprTopicEnqueuer<Context>(daprClient, "internal-orders", "hop", Options);

        var (root, spans) = await CaptureBlocksSpansAsync(() => enqueuer.ExecuteAsync(
            new SourceItem("order-1.json", "{}"u8.ToArray()), new Context(new Dictionary<string, string>()),
            CancellationToken.None));

        var send = Assert.Single(spans);
        Assert.Equal("send hop", send.DisplayName);
        Assert.Equal(ActivityKind.Producer, send.Kind);
        Assert.Equal(root.SpanId, send.ParentSpanId);
        Assert.Equal("dapr", send.GetTagItem("messaging.system"));
        Assert.Equal("send", send.GetTagItem("messaging.operation.type"));
        Assert.Equal("hop", send.GetTagItem("messaging.destination.name"));
        var traceParent = CloudEventAttribute.CreateExtension("traceparent", CloudEventAttributeType.String);
        var envelope = new JsonEventFormatter().DecodeStructuredModeMessage(published!, null, [traceParent]);
        Assert.Equal(send.Id, envelope[traceParent]);
        Assert.Equal(envelope.Id, send.GetTagItem("messaging.message.id"));
    }

    [Fact]
    public async Task ExecuteAsync_MarksTheSendSpanAsAnError_WhenPublishingFails()
    {
        var daprClient = Substitute.For<DaprClient>();
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("broker unavailable"));
        var enqueuer = new DaprTopicEnqueuer<Context>(daprClient, "internal-orders", "hop", Options);

        var (_, spans) = await CaptureBlocksSpansAsync(() => Assert.ThrowsAsync<InvalidOperationException>(() =>
            enqueuer.ExecuteAsync(new SourceItem("order-1.json", "{}"u8.ToArray()),
                new Context(new Dictionary<string, string>()), CancellationToken.None)));

        var send = Assert.Single(spans);
        Assert.Equal(ActivityStatusCode.Error, send.Status);
        Assert.Equal(typeof(InvalidOperationException).FullName, send.GetTagItem("error.type"));
        Assert.Contains(send.Events, e => e.Name == "exception");
    }

    /// <summary>Runs <paramref name="act"/> under a test-owned root span and returns it with the
    /// Blocks spans in its trace, ignoring spans from tests running in parallel.</summary>
    private static async Task<(Activity Root, List<Activity> Spans)> CaptureBlocksSpansAsync(Func<Task> act)
    {
        using var testSource = new ActivitySource($"enqueuer-test-{Guid.NewGuid()}");
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
