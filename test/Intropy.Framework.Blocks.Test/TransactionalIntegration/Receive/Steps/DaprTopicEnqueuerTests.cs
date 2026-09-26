using System.Text.Json;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using NSubstitute;

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
}
