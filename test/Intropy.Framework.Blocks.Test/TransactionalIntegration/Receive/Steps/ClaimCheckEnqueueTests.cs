using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Receive.Steps;

/// <summary>
/// The claim check: a payload too large for the broker goes to the claim store and the item carries a
/// pointer to it. A payload that fits is left alone, so the decorator can wrap every enqueuer.
/// </summary>
/// <remarks>
/// It decorates the enqueuer rather than preceding it in the pipeline, so what reaches the broker is
/// whatever it hands the enqueuer it wraps — which is what these assert on.
/// </remarks>
public class ClaimCheckEnqueueTests
{
    private static readonly FrameworkOptions Options =
        new() { ComponentName = "orders", ServiceNamespace = "example" };

    private static readonly TimeProvider Time =
        new FixedTimeProvider(new DateTimeOffset(2026, 3, 7, 11, 22, 33, TimeSpan.Zero));

    private static (ClaimCheckEnqueue<Context> Step, IFileAdapter Adapter, RecordingEnqueuer Inner)
        StepWith(ClaimCheckPolicy policy)
    {
        var adapter = Substitute.For<IFileAdapter>();
        var inner = new RecordingEnqueuer(Options);
        return (new ClaimCheckEnqueue<Context>(inner, adapter, policy, Options, Time), adapter, inner);
    }

    [Fact]
    public async Task ExecuteAsync_LeavesASmallPayloadAlone_AndStoresNothing()
    {
        var (step, adapter, inner) = StepWith(ClaimCheckPolicy.Above(4096));
        var item = new SourceItem("order-1.json", "{\"orderId\":\"ORD-1\"}"u8.ToArray()) { Port = "orders-in" };

        var (result, _) = await step.ExecuteAsync(item, new Context([]), CancellationToken.None);

        var success = Assert.IsType<TechnicalStepResult<SourceItem>.Success>(result);
        Assert.Same(item, success.Value);
        Assert.Null(success.Value.DataRef);
        await adapter.DidNotReceiveWithAnyArgs().WriteAsync(default!, default!, default, default);
    }

    [Fact]
    public async Task ExecuteAsync_StoresAnOversizedPayload_AndPointsTheItemAtIt()
    {
        var (step, adapter, inner) = StepWith(ClaimCheckPolicy.Above(1024));
        var payload = new byte[4096];
        Array.Fill(payload, (byte)0x5C);
        var item = new SourceItem("order-1.json", payload);

        var (result, _) = await step.ExecuteAsync(item, new Context([]), CancellationToken.None);

        var stored = Assert.IsType<TechnicalStepResult<SourceItem>.Success>(result).Value;
        Assert.NotNull(stored.MessageId);
        Assert.Equal($"orders/2026/03/07/{stored.MessageId}", stored.DataRef);
        // Cleared as well as pointed at: the envelope omits it either way, but an empty payload fails
        // small if that branch is ever wrong.
        Assert.Empty(stored.Data);
        await adapter.Received(1).WriteAsync(stored.DataRef!, Arg.Is<byte[]>(b => b.SequenceEqual(payload)),
            null, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_StoresEveryPayload_InAlwaysMode()
    {
        var (step, adapter, inner) = StepWith(ClaimCheckPolicy.Always);
        var item = new SourceItem("order-1.json", "{}"u8.ToArray());

        var (result, _) = await step.ExecuteAsync(item, new Context([]), CancellationToken.None);

        var stored = Assert.IsType<TechnicalStepResult<SourceItem>.Success>(result).Value;
        Assert.NotNull(stored.DataRef);
        await adapter.Received(1).WriteAsync(stored.DataRef!, Arg.Any<byte[]>(), null, Arg.Any<CancellationToken>());
    }

    // A caller-supplied id is kept rather than replaced, so the key names the message the pointer
    // actually travels on. It has to be one the key format can hold; the tests below pin what
    // happens when it is not.
    [Fact]
    public async Task ExecuteAsync_KeepsAnIdTheItemAlreadyCarries_SoTheKeyAndTheMessageAgree()
    {
        var (step, _, inner) = StepWith(ClaimCheckPolicy.Always);
        var item = new SourceItem("order-1.json", "{}"u8.ToArray()) { MessageId = "fixed-id" };

        var (result, _) = await step.ExecuteAsync(item, new Context([]), CancellationToken.None);

        var stored = Assert.IsType<TechnicalStepResult<SourceItem>.Success>(result).Value;
        Assert.Equal("fixed-id", stored.MessageId);
        Assert.Equal("orders/2026/03/07/fixed-id", stored.DataRef);
    }

    // Configuring a claim check with a component name the key format cannot hold fails when the
    // pipeline is composed, which for a Transactional Integration is startup — so the run never
    // begins rather than failing on its first oversized file.
    [Fact]
    public void WithClaimCheck_RefusesAComponentNameThatCannotNameAKey()
    {
        var builder = ReceivePipelineBuilder<Context>.Create("orders.Receive",
            new FrameworkOptions { ComponentName = "orders/eu", ServiceNamespace = "example" },
            NullLoggerFactory.Instance);

        var error = Assert.Throws<InvalidOperationException>(() =>
            builder.WithClaimCheck(Substitute.For<IFileAdapter>(), ClaimCheckPolicy.Always));

        Assert.Contains("orders/eu", error.Message, StringComparison.Ordinal);
    }

    // The framework's own sweep never sets MessageId, so this reaches the step only from component
    // code supplying its own SourceItem — but when it does, the refusal has to name the id rather
    // than surface as an ArgumentException about a key "segment" from inside ClaimKey.
    [Theory]
    [InlineData("urn:uuid:7c3f1a90-0f4e-4a1e-9a7e-2f3c5d6e7f80")]
    [InlineData("order:42")]
    [InlineData("order/42")]
    [InlineData("..")]
    public async Task ExecuteAsync_RefusesAMessageIdThatCannotNameAKey_NamingIt(string messageId)
    {
        var (step, adapter, inner) = StepWith(ClaimCheckPolicy.Always);
        var item = new SourceItem("order-1.json", "{}"u8.ToArray()) { MessageId = messageId };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            step.ExecuteAsync(item, new Context([]), CancellationToken.None));

        Assert.Contains(messageId, error.Message, StringComparison.Ordinal);
        Assert.Contains("message id", error.Message, StringComparison.Ordinal);
        await adapter.DidNotReceiveWithAnyArgs().WriteAsync(default!, default!, default, default);
    }

    // Checked before the size decision, so an id this integration cannot use is reported on its
    // first item rather than on the first one large enough to be claim-checked.
    [Fact]
    public async Task ExecuteAsync_RefusesAMessageIdThatCannotNameAKey_EvenWhenThePayloadWouldStayInline()
    {
        var (step, adapter, inner) = StepWith(ClaimCheckPolicy.Above(100_000));
        var item = new SourceItem("order-1.json", "{}"u8.ToArray()) { MessageId = "order:42" };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            step.ExecuteAsync(item, new Context([]), CancellationToken.None));

        await adapter.DidNotReceiveWithAnyArgs().WriteAsync(default!, default!, default, default);
    }

    // The control: the GUID the step mints for an item carrying no id always qualifies, which is
    // why the framework's own path never trips the check above.
    [Fact]
    public async Task ExecuteAsync_MintsAnIdThatCanNameAKey_WhenTheItemCarriesNone()
    {
        var (step, _, inner) = StepWith(ClaimCheckPolicy.Always);
        var item = new SourceItem("order-1.json", "{}"u8.ToArray());

        var (result, _) = await step.ExecuteAsync(item, new Context([]), CancellationToken.None);

        var stored = Assert.IsType<TechnicalStepResult<SourceItem>.Success>(result).Value;
        Assert.Equal($"orders/2026/03/07/{stored.MessageId}", stored.DataRef);
        Assert.True(ClaimKey.IsValid(stored.DataRef));
    }

    [Fact]
    public async Task ExecuteAsync_LetsAFailedWriteEscape_SoTheStepBecomesATechnicalFailure()
    {
        // The base TechnicalStep turns an escaping exception into a technical failure, which leaves the
        // source file uncompleted and the item to be swept again. Never a business failure.
        var (step, adapter, inner) = StepWith(ClaimCheckPolicy.Always);
        adapter.WriteAsync(default!, default!, default, default).ThrowsAsyncForAnyArgs(
            new InvalidOperationException("the claim store is unreachable"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            step.ExecuteAsync(new SourceItem("order-1.json", "{}"u8.ToArray()), new Context([]),
                CancellationToken.None));
    }

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    // The decorator's whole contract: whatever it decides, the item still reaches the enqueuer, and
    // reaches it exactly once. A claim check that swallowed an item would lose a source file.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ExecuteAsync_EnqueuesExactlyOnce_WhicheverWayTheSizeDecisionGoes(bool claimChecked)
    {
        var (step, _, inner) = StepWith(claimChecked ? ClaimCheckPolicy.Always : ClaimCheckPolicy.Above(100_000));
        var item = new SourceItem("order-1.json", "{}"u8.ToArray());

        await step.ExecuteAsync(item, new Context([]), CancellationToken.None);

        var enqueued = Assert.Single(inner.Enqueued);
        Assert.Equal(claimChecked, enqueued.DataRef is not null);
    }

    // The payload is written before the pointer is enqueued, so a failed write must stop the item
    // reaching the broker — a pointer to a payload that was never stored is unrecoverable.
    [Fact]
    public async Task ExecuteAsync_NeverEnqueues_WhenTheStoreWriteFails()
    {
        var (step, adapter, inner) = StepWith(ClaimCheckPolicy.Always);
        adapter.WriteAsync(default!, default!, default, default)
            .ThrowsAsyncForAnyArgs(new InvalidOperationException("the store is unreachable"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            step.ExecuteAsync(new SourceItem("order-1.json", "{}"u8.ToArray()), new Context([]),
                CancellationToken.None));

        Assert.Empty(inner.Enqueued);
    }

    // The step the pipeline reports is the enqueuer's, not the claim check's: the item really is
    // being enqueued, and the claim check's own work is attributable through the span ClaimWriter
    // starts around the store write.
    [Fact]
    public void StepName_IsTheEnqueuersOwn()
    {
        var (step, _, inner) = StepWith(ClaimCheckPolicy.Always);

        Assert.Equal(inner.StepName, step.StepName);
    }


    /// <summary>The enqueuer the claim check wraps, recording the item it was handed. Its envelope
    /// is built by the real base-class body, so these tests exercise the production path the
    /// decorator delegates into.</summary>
    private sealed class RecordingEnqueuer(FrameworkOptions options) : EnqueueStep<Context>(options)
    {
        public List<SourceItem> Enqueued { get; } = [];

        public override Task<(TechnicalStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
            SourceItem input, ReadOnlyMemory<byte> cloudEvent, Context context, CancellationToken ct)
        {
            Enqueued.Add(input);
            return Task.FromResult<(TechnicalStepResult<SourceItem>, Context)>(
                (new TechnicalStepResult<SourceItem>.Success(input), context));
        }
    }

}
