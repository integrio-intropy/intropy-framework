using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Intropy.Framework.Blocks.Test.Extractor.Steps;

/// <summary>
/// The extractor's claim check: a payload too large for the broker is written to the claim store and
/// the event published carries a pointer to it instead. Below the threshold the event goes out
/// untouched, so the decorator needs no conditional composition.
/// </summary>
public class ClaimCheckSendTests
{
    private static readonly Uri Source = new("urn:test:orders");
    private const string Type = "com.example.order.extracted";
    private const string Component = "orders";

    private static readonly FrameworkOptions Options =
        new() { ComponentName = Component, ServiceNamespace = "example" };

    private static readonly DateTimeOffset Now = new(2026, 3, 7, 11, 0, 0, TimeSpan.Zero);
    private const string ExpectedKey = $"{Component}/2026/03/07/evt-1";

    private sealed record Payload(string OrderId, int Quantity);

    /// <summary>Captures what reached the real send step, which is the only place that can tell
    /// whether the event was claim-checked before it went out.</summary>
    private sealed class CapturingSender : SendStep<Context>
    {
        public CloudEvent? Sent { get; private set; }

        public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(
            CloudEvent input, Context context, CancellationToken ct)
        {
            Sent = input;
            return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
                (new TechnicalStepResult<CloudEvent>.Success(input), context));
        }
    }

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private static (ClaimCheckSend<Context> Step, CapturingSender Sender, IFileAdapter Store) StepWith(
        ClaimCheckPolicy policy)
    {
        var sender = new CapturingSender();
        var store = Substitute.For<IFileAdapter>();
        return (new ClaimCheckSend<Context>(sender, store, policy, Options, (Source, Type),
            new FixedTimeProvider(Now)), sender, store);
    }

    /// <summary>An event as the serialize step hands it over: id and subject set, payload an object,
    /// source and type still the send step's to supply.</summary>
    private static CloudEvent Event(object? data, string? id = "evt-1", string contentType = "application/json") =>
        new()
        {
            Id = id,
            Subject = "ORD-1",
            Time = Now,
            DataContentType = contentType,
            Data = data
        };

    private static string? DataRef(CloudEvent cloudEvent) =>
        cloudEvent.GetPopulatedAttributes()
            .FirstOrDefault(a => a.Key.Name == CloudEventExtensionKeys.DataRef).Value as string;

    [Fact]
    public async Task ExecuteAsync_LeavesASmallPayloadAlone_AndStoresNothing()
    {
        var (step, sender, store) = StepWith(ClaimCheckPolicy.Above(100_000));
        var input = Event(new Payload("ORD-1", 3));

        var (result, _) = await step.ExecuteAsync(input, new Context([], false), CancellationToken.None);

        Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(result);
        Assert.Null(DataRef(sender.Sent!));
        Assert.NotNull(sender.Sent!.Data);
        await store.DidNotReceiveWithAnyArgs().WriteAsync(default!, default!, default, default);
    }

    [Fact]
    public async Task ExecuteAsync_StoresAnOversizedPayload_AndPointsTheEnvelopeAtIt()
    {
        var (step, sender, store) = StepWith(ClaimCheckPolicy.Above(200));
        var input = Event(new Payload(new string('x', 500), 3));

        await step.ExecuteAsync(input, new Context([], false), CancellationToken.None);

        Assert.Equal(ExpectedKey, DataRef(sender.Sent!));
        // Cleared as well as pointed at, so a wrong branch here publishes a small message rather
        // than the oversized one the mechanism exists to prevent.
        Assert.Null(sender.Sent!.Data);
        await store.Received(1).WriteAsync(ExpectedKey, Arg.Any<byte[]>(), null, Arg.Any<CancellationToken>());
    }

    // The invariant the whole feature turns on: a consumer deserializes the same JSON whether the
    // payload arrived inline or came back out of the store. The claim check must therefore write
    // exactly the bytes the formatter would have put in the `data` member — camelCase included.
    [Fact]
    public async Task ExecuteAsync_StoresTheBytesTheFormatterWouldHaveWritten_SoAConsumerReadsTheSameJsonEitherWay()
    {
        var (step, _, store) = StepWith(ClaimCheckPolicy.Always);
        byte[]? written = null;
        await store.WriteAsync(Arg.Any<string>(), Arg.Do<byte[]>(b => written = b), Arg.Any<string?>(),
            Arg.Any<CancellationToken>());

        var payload = new Payload("ORD-1", 3);
        await step.ExecuteAsync(Event(payload), new Context([], false), CancellationToken.None);

        Assert.Equal("""{"orderId":"ORD-1","quantity":3}""", Encoding.UTF8.GetString(written!));

        // And the same bytes the encoder itself produces for an inline event's data member.
        using var inline = JsonDocument.Parse(
            CloudEventFormat.Formatter.EncodeStructuredModeMessage(
                new CloudEvent
                {
                    Id = "evt-1", Source = Source, Type = Type, Subject = "ORD-1", Time = Now,
                    DataContentType = "application/json", Data = payload
                }, out _));
        Assert.Equal(inline.RootElement.GetProperty("data").GetRawText(), Encoding.UTF8.GetString(written!));
    }

    /// <summary>The <c>data</c> (or <c>data_base64</c>) member the formatter writes for
    /// <paramref name="payload"/>, as its raw JSON text.</summary>
    private static string InlineDataMember(object payload, out bool base64)
    {
        using var inline = JsonDocument.Parse(CloudEventFormat.Formatter.EncodeStructuredModeMessage(
            new CloudEvent
            {
                Id = "evt-1", Source = Source, Type = Type, Subject = "ORD-1", Time = Now,
                DataContentType = "application/json", Data = payload
            }, out _).ToArray());
        base64 = inline.RootElement.TryGetProperty("data_base64", out var b64);
        return (base64 ? b64 : inline.RootElement.GetProperty("data")).GetRawText();
    }

    private static async Task<byte[]> StoredBytesAsync(object payload)
    {
        var (step, _, store) = StepWith(ClaimCheckPolicy.Always);
        byte[]? written = null;
        await store.WriteAsync(Arg.Any<string>(), Arg.Do<byte[]>(b => written = b), Arg.Any<string?>(),
            Arg.Any<CancellationToken>());
        await step.ExecuteAsync(Event(payload), new Context([], false), CancellationToken.None);
        return written!;
    }

    // A ReadOnlyMemory<byte> is not a shape the formatter moves to data_base64 — it has no case for
    // it, so it serializes like any other value and lands in `data` as a quoted base64 string.
    // Storing its raw bytes would hand the consumer something different depending only on size.
    [Fact]
    public async Task ExecuteAsync_StoresAReadOnlyMemoryPayload_AsTheQuotedBase64TheFormatterWrites()
    {
        var payload = new ReadOnlyMemory<byte>("abc"u8.ToArray());

        var written = await StoredBytesAsync(payload);

        Assert.Equal("\"YWJj\"", Encoding.UTF8.GetString(written));
        Assert.Equal(InlineDataMember(payload, out var base64), Encoding.UTF8.GetString(written));
        Assert.False(base64);
    }

    // The control: a byte[] is the one shape that does become data_base64, and there the consumer
    // gets the decoded bytes — so the store holds them raw.
    [Fact]
    public async Task ExecuteAsync_StoresAByteArrayPayload_Raw_BecauseTheFormatterBase64sItSeparately()
    {
        var payload = "abc"u8.ToArray();

        var written = await StoredBytesAsync(payload);

        Assert.Equal("abc", Encoding.UTF8.GetString(written));
        Assert.Equal("\"YWJj\"", InlineDataMember(payload, out var base64));
        Assert.True(base64);
    }

    // A byte[] payload is the one shape the formatter encodes as data_base64, which costs a third
    // more on the wire — so it has to be measured as base64, not as its raw length.
    [Fact]
    public async Task ExecuteAsync_CountsABinaryPayloadAsBase64()
    {
        // 600 raw bytes encode to 800; the envelope overhead is well under 200, so a threshold of
        // 800 is above the raw length and below the encoded one.
        var (step, sender, _) = StepWith(ClaimCheckPolicy.Above(800));

        await step.ExecuteAsync(Event(new byte[600]), new Context([], false), CancellationToken.None);

        Assert.Equal(ExpectedKey, DataRef(sender.Sent!));
    }

    // The key names the message the pointer travels on, so an event without an id gets one here —
    // CloudEventSerializeStep mints one, but a component-supplied serialize step need not.
    [Fact]
    public async Task ExecuteAsync_MintsAnId_WhenTheSerializeStepLeftNone()
    {
        var (step, sender, _) = StepWith(ClaimCheckPolicy.Always);

        await step.ExecuteAsync(Event(new Payload("ORD-1", 3), id: null), new Context([], false),
            CancellationToken.None);

        Assert.False(string.IsNullOrEmpty(sender.Sent!.Id));
        Assert.Equal($"{Component}/2026/03/07/{sender.Sent.Id}", DataRef(sender.Sent));
    }

    // The failure mode this guards is a latent one: an id outside the key's charset publishes small
    // events perfectly well and only breaks once one crosses the threshold, so the message has to
    // name the id rather than talk about key segments.
    [Theory]
    [InlineData("urn:uuid:7c3f1a90-0f4e-4a1e-9a7e-2f3c5d6e7f80")]
    [InlineData("order:42")]
    [InlineData("order/42")]
    [InlineData("..")]
    public async Task ExecuteAsync_RefusesAnIdThatCannotNameAKey_NamingTheId(string id)
    {
        var (step, sender, store) = StepWith(ClaimCheckPolicy.Always);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            step.ExecuteAsync(Event(new Payload("ORD-1", 3), id), new Context([], false), CancellationToken.None));

        Assert.Contains(id, error.Message, StringComparison.Ordinal);
        Assert.Contains("claim-checked", error.Message, StringComparison.Ordinal);
        Assert.Null(sender.Sent);
        await store.DidNotReceiveWithAnyArgs().WriteAsync(default!, default!, default, default);
    }

    // Checked before the size decision, so a payload that would never be claim-checked is refused
    // too: the id scheme is a configuration mistake, and this pipeline finds it on its first event
    // rather than on the first one large enough to cross the threshold.
    [Fact]
    public async Task ExecuteAsync_RefusesAnIdThatCannotNameAKey_EvenWhenThePayloadWouldStayInline()
    {
        var (step, sender, store) = StepWith(ClaimCheckPolicy.Above(100_000));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            step.ExecuteAsync(Event(new Payload("ORD-1", 3), "order:42"), new Context([], false),
                CancellationToken.None));

        Assert.Null(sender.Sent);
        await store.DidNotReceiveWithAnyArgs().WriteAsync(default!, default!, default, default);
    }

    // The control: an id of the shape the built-in serialize step mints goes through untouched.
    [Fact]
    public async Task ExecuteAsync_AcceptsAGuidId_AsTheBuiltInSerializeStepMints()
    {
        var (step, sender, _) = StepWith(ClaimCheckPolicy.Always);
        var id = Guid.NewGuid().ToString();

        await step.ExecuteAsync(Event(new Payload("ORD-1", 3), id), new Context([], false), CancellationToken.None);

        Assert.Equal($"{Component}/2026/03/07/{id}", DataRef(sender.Sent!));
    }

    // A real sidecar hands a subscriber the raw text of a string payload whose content type it does
    // not read as JSON, rather than the JSON string the formatter wrote — pinned in
    // LoaderCallbackDaprTests.AStringPayload_IsDeliveredRaw_UnlessItsContentTypeIsJson. The store has
    // to hold the same thing, or the same pipeline would hand its consumer different bytes
    // depending only on the payload's size.
    [Theory]
    [InlineData("text/plain")]
    [InlineData("text/csv")]
    [InlineData("application/xml")]
    public async Task ExecuteAsync_StoresAStringPayloadRaw_WhenItsContentTypeIsNotJson(string contentType)
    {
        var (step, _, store) = StepWith(ClaimCheckPolicy.Always);
        byte[]? written = null;
        await store.WriteAsync(Arg.Any<string>(), Arg.Do<byte[]>(b => written = b), Arg.Any<string?>(),
            Arg.Any<CancellationToken>());

        await step.ExecuteAsync(Event("a,b\nc,d", contentType: contentType), new Context([], false),
            CancellationToken.None);

        Assert.Equal("a,b\nc,d", Encoding.UTF8.GetString(written!));
    }

    // The control: under a JSON content type the sidecar leaves the JSON string alone, so the store
    // holds the quoted form the formatter wrote.
    [Theory]
    [InlineData("application/json")]
    [InlineData("application/vnd.acme.order+json")]
    [InlineData("application/json; charset=utf-8")]
    public async Task ExecuteAsync_StoresAStringPayloadQuoted_WhenItsContentTypeIsJson(string contentType)
    {
        var (step, _, store) = StepWith(ClaimCheckPolicy.Always);
        byte[]? written = null;
        await store.WriteAsync(Arg.Any<string>(), Arg.Do<byte[]>(b => written = b), Arg.Any<string?>(),
            Arg.Any<CancellationToken>());

        await step.ExecuteAsync(Event("a,b\nc,d", contentType: contentType), new Context([], false),
            CancellationToken.None);

        Assert.Equal("\"a,b\\nc,d\"", Encoding.UTF8.GetString(written!));
    }

    // The threshold measures the envelope, which carries the JSON string whatever the content type
    // — so a raw-stored payload is still counted at its encoded cost, not its shorter raw length.
    [Fact]
    public async Task ExecuteAsync_MeasuresAStringPayload_AtItsEncodedCost_NotItsRawLength()
    {
        // 300 quote characters encode to 600 bytes of \" escapes; the envelope overhead is well
        // under 100, so a threshold of 500 sits above the raw length and below the encoded one.
        var (step, sender, _) = StepWith(ClaimCheckPolicy.Above(500));

        await step.ExecuteAsync(Event(new string('"', 300), contentType: "text/plain"), new Context([], false),
            CancellationToken.None);

        Assert.Equal(ExpectedKey, DataRef(sender.Sent!));
    }

    [Fact]
    public async Task ExecuteAsync_StoresEveryPayload_InAlwaysMode()
    {
        var (step, sender, store) = StepWith(ClaimCheckPolicy.Always);

        await step.ExecuteAsync(Event(new Payload("A", 1)), new Context([], false), CancellationToken.None);

        Assert.Equal(ExpectedKey, DataRef(sender.Sent!));
        await store.Received(1).WriteAsync(ExpectedKey, Arg.Any<byte[]>(), null, Arg.Any<CancellationToken>());
    }

    // Writing the payload before publishing the pointer is what makes a crash leave an orphaned
    // payload rather than a pointer to one that was never written — so a failed write must stop the
    // send, not fall through to it.
    [Fact]
    public async Task ExecuteAsync_LetsAFailedWriteEscape_SoNoPointerIsPublishedWithoutItsPayload()
    {
        var (step, sender, store) = StepWith(ClaimCheckPolicy.Always);
        store.WriteAsync(default!, default!, default, default).ThrowsAsyncForAnyArgs(
            new InvalidOperationException("the store is unreachable"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            step.ExecuteAsync(Event(new Payload("A", 1)), new Context([], false), CancellationToken.None));

        Assert.Null(sender.Sent);
    }

    // The envelope the policy measures must be the one that goes out: the decorator completes it
    // before measuring, because encoding refuses an envelope missing source or type.
    [Fact]
    public async Task ExecuteAsync_CompletesTheEnvelope_SoItCanBeMeasuredAtAll()
    {
        var (step, sender, _) = StepWith(ClaimCheckPolicy.Above(100_000));

        await step.ExecuteAsync(Event(new Payload("A", 1)), new Context([], false), CancellationToken.None);

        Assert.Equal(Source, sender.Sent!.Source);
        Assert.Equal(Type, sender.Sent.Type);
    }

    // An event with no payload has nothing to claim-check; it must still reach the sender.
    [Fact]
    public async Task ExecuteAsync_PassesAnEventWithNoPayloadStraightThrough()
    {
        var (step, sender, store) = StepWith(ClaimCheckPolicy.Always);

        await step.ExecuteAsync(Event(null), new Context([], false), CancellationToken.None);

        Assert.NotNull(sender.Sent);
        Assert.Null(DataRef(sender.Sent!));
        await store.DidNotReceiveWithAnyArgs().WriteAsync(default!, default!, default, default);
    }
}
