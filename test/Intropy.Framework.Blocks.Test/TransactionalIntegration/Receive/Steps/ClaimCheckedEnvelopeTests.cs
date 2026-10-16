using System.Text.Json;
using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Receive.Steps;

/// <summary>
/// The envelope a claim-checked item is published in: the pointer travels, the payload does not. This
/// is the invariant the whole mechanism rests on — if data were still on the wire, nothing would have
/// been gained.
/// </summary>
public class ClaimCheckedEnvelopeTests
{
    private static readonly FrameworkOptions Options =
        new() { ComponentName = "orders", ServiceNamespace = "example" };

    private static async Task<JsonElement> PublishAsync(SourceItem item)
    {
        var daprClient = Substitute.For<DaprClient>();
        byte[] published = [];
        daprClient
            .PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(ci => published = ci.ArgAt<ReadOnlyMemory<byte>>(2).ToArray());
        var enqueuer = new DaprTopicEnqueuer<Context>(daprClient, "pubsub", "internal-orders", Options);

        await enqueuer.ExecuteAsync(item, new Context([]), CancellationToken.None);

        using var document = JsonDocument.Parse(published);
        return document.RootElement.Clone();
    }

    [Fact]
    public async Task AClaimCheckedItem_CarriesThePointerAndNoPayload()
    {
        var envelope = await PublishAsync(new SourceItem("order-1.json", [])
        {
            MessageId = "msg-1",
            DataRef = "orders/2026/03/07/msg-1"
        });

        Assert.Equal("orders/2026/03/07/msg-1", envelope.GetProperty(CloudEventExtensionKeys.DataRef).GetString());
        Assert.False(envelope.TryGetProperty("data", out _));
        Assert.False(envelope.TryGetProperty("data_base64", out _));
        // The id the claim key names, so a dead-lettered message leads back to its payload.
        Assert.Equal("msg-1", envelope.GetProperty("id").GetString());
    }

    [Fact]
    public async Task AnInlineItem_CarriesItsPayloadAndNoPointer()
    {
        var envelope = await PublishAsync(new SourceItem("order-1.json", "{\"orderId\":\"ORD-1\"}"u8.ToArray()));

        Assert.False(envelope.TryGetProperty(CloudEventExtensionKeys.DataRef, out _));
        Assert.Equal("{\"orderId\":\"ORD-1\"}",
            System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(
                envelope.GetProperty("data_base64").GetString()!)));
    }
}
