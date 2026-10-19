using System.Text.Json;
using System.Text.RegularExpressions;
using CloudNative.CloudEvents;
using Dapr.Client;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Adapters.Common;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Test.Extractor;

/// <summary>
/// A whole extractor pipeline with the claim check configured: what it actually puts on the wire.
/// The envelope a claim-checked payload travels in carries a <c>dataref</c> and no <c>data</c> at
/// all, which is what a consumer resolves — and what makes a <c>data.*</c> subscription filter stop
/// matching it.
/// </summary>
public class ExtractorClaimCheckTests
{
    private sealed record Order(string OrderId, string Notes);

    private readonly RecordingStore _store = new();
    private readonly IServiceProvider _provider;
    private byte[]? _published;

    public ExtractorClaimCheckTests()
    {
        var idempotency = Substitute.For<IIdempotencyServiceClient>();
        idempotency.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));

        var daprClient = Substitute.For<DaprClient>();
        daprClient.PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(ci => _published = ci.ArgAt<ReadOnlyMemory<byte>>(2).ToArray());

        var services = new ServiceCollection();
        services.AddSingleton(idempotency);
        services.AddSingleton(Substitute.For<IBusinessIncidentServiceClient>());
        services.AddSingleton(daprClient);
        services.AddLogging();
        services.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "orders";
            conf.ServiceNamespace = "example";
        });
        _provider = services.BuildServiceProvider();
    }

    private Extractor<Order, Order, Context> Build(ClaimCheckPolicy? policy)
    {
        var builder = ExtractorBuilder<Order, Order, Context>.Create("orders.Process", _provider)
            .WithDeserializer(new OrderDeserializer())
            .WithValidator(new PassingValidator())
            .WithIdempotency((o, _) => o.OrderId, (_, _) => DateTimeOffset.UtcNow)
            .WithTransformer(new IdentityTransformer())
            .WithSerializer(new CloudEventSerializeStep<Order, Context>(o => o.OrderId, _ => DateTimeOffset.UtcNow))
            .WithDaprTopicPublisher("pubsub", "orders", new Uri("urn:example:orders"), "com.example.order")
            .WithBusinessIncidents(_ => "orders", _ => "orders");
        if (policy is not null)
            builder = builder.WithClaimCheck(_store, policy);
        return builder.Build();
    }

    private static ReadOnlyMemory<byte> Input(Order order) => JsonSerializer.SerializeToUtf8Bytes(order);

    [Fact]
    public async Task AClaimCheckedPayload_TravelsAsAPointer_AndTheStoreHoldsTheBytesTheConsumerNeeds()
    {
        var order = new Order("ORD-1", new string('x', 4_000));

        var (result, _) = await Build(ClaimCheckPolicy.Above(1_000))
            .Execute(Input(order), new Context([], false), ct: CancellationToken.None);

        Assert.IsType<StepResult<CloudEvent>.Success>(result);
        using var envelope = JsonDocument.Parse(_published!);
        var key = envelope.RootElement.GetProperty(CloudEventExtensionKeys.DataRef).GetString();
        // The key's shape, not a date computed here: reading the clock a second time would make the
        // test flake across midnight UTC, and in a custom format string "/" is the current culture's
        // date separator, which is "-" on sv-SE. ClaimCheckSendTests pins the exact date against a
        // fixed TimeProvider; what matters at this level is that the component and the date
        // partition reach the key at all.
        var id = Regex.Escape(envelope.RootElement.GetProperty("id").GetString()!);
        Assert.Matches("^orders/[0-9]{4}/[0-9]{2}/[0-9]{2}/" + id + "$", key);
        Assert.False(envelope.RootElement.TryGetProperty("data", out _));
        Assert.False(envelope.RootElement.TryGetProperty("data_base64", out _));

        // What the store holds is what the consumer deserializes, so it must be the payload itself.
        var stored = JsonSerializer.Deserialize<Order>(_store.Files[key!], CloudEventFormat.SerializerOptions);
        Assert.Equal(order, stored);
    }

    // The control: the same pipeline, the same store, a payload under the threshold.
    [Fact]
    public async Task AnInlinePayload_CarriesItsDataAndNoPointer()
    {
        var order = new Order("ORD-2", "short");

        await Build(ClaimCheckPolicy.Above(100_000))
            .Execute(Input(order), new Context([], false), ct: CancellationToken.None);

        using var envelope = JsonDocument.Parse(_published!);
        Assert.False(envelope.RootElement.TryGetProperty(CloudEventExtensionKeys.DataRef, out _));
        Assert.Equal("ORD-2", envelope.RootElement.GetProperty("data").GetProperty("orderId").GetString());
        Assert.Empty(_store.Files);
    }

    // Without WithClaimCheck the pipeline is exactly what it was before the feature existed.
    [Fact]
    public async Task WithoutAClaimCheck_AnOversizedPayloadStillRidesInline()
    {
        var order = new Order("ORD-3", new string('x', 4_000));

        await Build(null).Execute(Input(order), new Context([], false), ct: CancellationToken.None);

        using var envelope = JsonDocument.Parse(_published!);
        Assert.False(envelope.RootElement.TryGetProperty(CloudEventExtensionKeys.DataRef, out _));
        Assert.Empty(_store.Files);
    }

    // The claim check measures the encoded envelope, which needs the source and type the send step
    // supplies. A custom sender that declares neither cannot be measured, so this has to fail at
    // composition — not days later, on the first payload large enough to need the store.
    [Fact]
    public void AClaimCheckOnASenderDeclaringNoEnvelopeDefaults_FailsWhenTheExtractorIsBuilt()
    {
        var builder = ExtractorBuilder<Order, Order, Context>.Create("orders.Process", _provider)
            .WithDeserializer(new OrderDeserializer())
            .WithValidator(new PassingValidator())
            .WithIdempotency((o, _) => o.OrderId, (_, _) => DateTimeOffset.UtcNow)
            .WithTransformer(new IdentityTransformer())
            .WithSerializer(new CloudEventSerializeStep<Order, Context>(o => o.OrderId, _ => DateTimeOffset.UtcNow))
            .WithSender(new DiscardingSender())
            .WithBusinessIncidents(_ => "orders", _ => "orders")
            .WithClaimCheck(_store, ClaimCheckPolicy.Always);

        var error = Assert.Throws<InvalidOperationException>(() => builder.Build());

        Assert.Contains(nameof(DiscardingSender), error.Message, StringComparison.Ordinal);
        Assert.Contains("EnvelopeDefaults", error.Message, StringComparison.Ordinal);
    }

    private sealed class OrderDeserializer : DeserializeStep<Order, Context>
    {
        public override Task<(BusinessStepResult<Order> Result, Context Context)> ExecuteAsync(
            ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<Order>, Context)>(
                (new BusinessStepResult<Order>.Success(JsonSerializer.Deserialize<Order>(input.Span)!), context));
    }

    private sealed class PassingValidator : ValidateStep<Order, Context>
    {
        public override Task<(BusinessStepResult<Order> Result, Context Context)> ExecuteAsync(
            Order input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<Order>, Context)>(
                (new BusinessStepResult<Order>.Success(input), context));
    }

    private sealed class IdentityTransformer : TransformStep<Order, Order, Context>
    {
        public override Task<(TechnicalStepResult<Order> Result, Context Context)> ExecuteAsync(
            Order input, Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<Order>, Context)>(
                (new TechnicalStepResult<Order>.Success(input), context));
    }

    /// <summary>The claim store, recording what was written under which key. Blocks.Test does not
    /// reference Intropy.Framework.Testing, and the assertions here are about the bytes.</summary>
    private sealed class RecordingStore : IFileAdapter
    {
        public Dictionary<string, byte[]> Files { get; } = new(StringComparer.Ordinal);

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null,
            CancellationToken ct = default)
        {
            Files[fileName] = content;
            return Task.CompletedTask;
        }

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default) =>
            Task.FromResult(Files[fileName]);

        public Task<List<FileEntry>> ListAsync(CancellationToken ct = default) =>
            Task.FromResult(new List<FileEntry>());

        public Task DeleteAsync(string fileName, CancellationToken ct = default) => Task.CompletedTask;
    }

    private sealed class DiscardingSender : SendStep<Context>
    {
        public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(
            CloudEvent input, Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
                (new TechnicalStepResult<CloudEvent>.Success(input), context));
    }
}
