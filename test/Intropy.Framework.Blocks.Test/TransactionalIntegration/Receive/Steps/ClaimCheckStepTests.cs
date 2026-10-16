using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Receive.Steps;

/// <summary>
/// The claim check: a payload too large for the broker goes to the claim store and the item carries a
/// pointer to it. A payload that fits is left alone, so the step can sit in every pipeline.
/// </summary>
public class ClaimCheckStepTests
{
    private static readonly FrameworkOptions Options =
        new() { ComponentName = "orders", ServiceNamespace = "example" };

    private static readonly TimeProvider Time =
        new FixedTimeProvider(new DateTimeOffset(2026, 3, 7, 11, 22, 33, TimeSpan.Zero));

    private static (ClaimCheckStep<Context> Step, IFileAdapter Adapter) StepWith(ClaimCheckPolicy policy)
    {
        var adapter = Substitute.For<IFileAdapter>();
        return (new ClaimCheckStep<Context>(adapter, policy, Options, Time), adapter);
    }

    [Fact]
    public async Task ExecuteAsync_LeavesASmallPayloadAlone_AndStoresNothing()
    {
        var (step, adapter) = StepWith(ClaimCheckPolicy.Above(4096));
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
        var (step, adapter) = StepWith(ClaimCheckPolicy.Above(1024));
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
        var (step, adapter) = StepWith(ClaimCheckPolicy.Always);
        var item = new SourceItem("order-1.json", "{}"u8.ToArray());

        var (result, _) = await step.ExecuteAsync(item, new Context([]), CancellationToken.None);

        var stored = Assert.IsType<TechnicalStepResult<SourceItem>.Success>(result).Value;
        Assert.NotNull(stored.DataRef);
        await adapter.Received(1).WriteAsync(stored.DataRef!, Arg.Any<byte[]>(), null, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_KeepsAnIdTheItemAlreadyCarries_SoTheKeyAndTheMessageAgree()
    {
        var (step, _) = StepWith(ClaimCheckPolicy.Always);
        var item = new SourceItem("order-1.json", "{}"u8.ToArray()) { MessageId = "fixed-id" };

        var (result, _) = await step.ExecuteAsync(item, new Context([]), CancellationToken.None);

        var stored = Assert.IsType<TechnicalStepResult<SourceItem>.Success>(result).Value;
        Assert.Equal("fixed-id", stored.MessageId);
        Assert.Equal("orders/2026/03/07/fixed-id", stored.DataRef);
    }

    [Fact]
    public async Task ExecuteAsync_LetsAFailedWriteEscape_SoTheStepBecomesATechnicalFailure()
    {
        // The base TechnicalStep turns an escaping exception into a technical failure, which leaves the
        // source file uncompleted and the item to be swept again. Never a business failure.
        var (step, adapter) = StepWith(ClaimCheckPolicy.Always);
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
}
