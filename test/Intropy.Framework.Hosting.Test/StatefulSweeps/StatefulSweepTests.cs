using Intropy.Framework.Testing.StatefulSweeps;
using Intropy.Framework.Adapters.File;
using NSubstitute;
using System.Text;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Sweeps;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.StatefulSweeps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Test.StatefulSweeps;

/// <summary>
/// The cursor protocol of a polled source: a full export keeps no cursor and no key; a delta pins its
/// upper bound as pending before the message is handled, commits it only after, re-reads exactly the
/// pinned range after a failure with the same idempotency key, and never moves the cursor for a
/// message that was not handled.
/// </summary>
public class StatefulSweepTests
{
    private const string Port = "campaigns";
    private const string Store = "watermarks";
    private const string Key = "test-component:campaigns";

    [Fact]
    public async Task FullExport_HandsOverTheContent_WithoutAKeyOrAFile_AndKeepsNoCursor()
    {
        var source = new FakeSource(cursor: null) { Reads = { PolledResult.Page([new PolledItem(Bytes("[1]"))], null) } };
        var store = new InMemoryCursorStore();
        var handled = new List<SweptItem>();

        var summary = await Sweep(source, store).SweepAsync(Handle(handled, SweepOutcome.Consumed), CancellationToken.None);

        Assert.Equal(new RunSummary(1, 0, 0), summary);
        var item = Assert.Single(handled);
        Assert.Equal("[1]", Encoding.UTF8.GetString(await item.ReadAsync()));
        Assert.IsNotType<SweptFile>(item);
        Assert.Empty(item.Metadata);
        Assert.Equal(Port, item.Port);
        Assert.DoesNotContain(SourceContextKeys.IdempotencyKey, item.Metadata.Keys);
        Assert.Equal(CursorRange.All, Assert.Single(source.Ranges));
        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task Delta_FirstRun_ReadsFromTheInitialCursor_PinsBeforeHandling_AndCommitsAfter()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { PolledResult.Page([new PolledItem(Bytes("[1]"))], "w1") } };
        var store = new InMemoryCursorStore();
        CursorState? whileHandling = null;

        var summary = await Sweep(source, store).SweepAsync(async (item, _) =>
        {
            whileHandling = (await store.GetAsync(Key, CancellationToken.None))?.State;
            Assert.Equal("campaigns:w0:w1", item.Metadata[SourceContextKeys.IdempotencyKey]);
            return SweepOutcome.Consumed;
        }, CancellationToken.None);

        Assert.Equal(new RunSummary(1, 0, 0), summary);
        Assert.Equal(new CursorRange("w0", null), Assert.Single(source.Ranges));
        Assert.Equal(new CursorState("w0", "w1"), whileHandling);
        Assert.Equal(new CursorState("w1", null), store.State[Key]);
    }

    [Fact]
    public async Task Delta_AfterAFailedRun_ReadsExactlyThePinnedRangeAgain_WithTheSameKey()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0"))
        {
            Reads = { PolledResult.Page([new PolledItem(Bytes("[1]"))], "w1"), PolledResult.Page([new PolledItem(Bytes("[1,2]"))], "w2") }
        };
        var store = new InMemoryCursorStore();
        var handled = new List<SweptItem>();

        var first = await Sweep(source, store).SweepAsync(Handle(handled, SweepOutcome.Failed), CancellationToken.None);
        Assert.Equal(new RunSummary(0, 1, 0), first);
        Assert.Equal(new CursorState("w0", "w1"), store.State[Key]);

        var second = await Sweep(source, store).SweepAsync(Handle(handled, SweepOutcome.Consumed), CancellationToken.None);

        Assert.Equal(new RunSummary(1, 0, 0), second);
        // The second read is pinned to the first one's bound, whatever the source would report now.
        Assert.Equal([new CursorRange("w0", null), new CursorRange("w0", "w1")], source.Ranges);
        Assert.Equal(handled[0].Metadata[SourceContextKeys.IdempotencyKey], handled[1].Metadata[SourceContextKeys.IdempotencyKey]);
        Assert.Equal(new CursorState("w1", null), store.State[Key]);
    }

    [Fact]
    public async Task Delta_ADuplicate_IsSkipped_AndCommits()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { PolledResult.Page([new PolledItem(Bytes("[1]"))], "w1") } };
        var store = new InMemoryCursorStore();
        store.Set(Key, new CursorState("w0", "w1"));

        var summary = await Sweep(source, store).SweepAsync(Handle([], SweepOutcome.Duplicate), CancellationToken.None);

        Assert.Equal(new RunSummary(0, 0, 1), summary);
        Assert.Equal(new CursorState("w1", null), store.State[Key]);
    }

    [Fact]
    public async Task Delta_WhenCommittingFails_CountsAFailure_AndKeepsTheRangePending()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { PolledResult.Page([new PolledItem(Bytes("[1]"))], "w1") } };
        var store = new InMemoryCursorStore { SaveFailure = state => state.Pending is null ? new IOException("state store unreachable") : null };

        var summary = await Sweep(source, store).SweepAsync(Handle([], SweepOutcome.Consumed), CancellationToken.None);

        Assert.Equal(new RunSummary(0, 1, 0), summary);
        Assert.Equal(new CursorState("w0", "w1"), store.State[Key]);
    }

    [Fact]
    public async Task NothingToSend_SavesNothing()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { PolledResult.Empty } };
        var store = new InMemoryCursorStore();

        var summary = await Sweep(source, store).SweepAsync(Handle([], SweepOutcome.Consumed), CancellationToken.None);

        Assert.Equal(RunSummary.Empty, summary);
        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task APinnedRangeWithNothingLeftToSend_IsCommitted_SoItIsNotReadForever()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { PolledResult.Empty } };
        var store = new InMemoryCursorStore();
        store.Set(Key, new CursorState("w0", "w1"));

        var summary = await Sweep(source, store).SweepAsync(Handle([], SweepOutcome.Consumed), CancellationToken.None);

        Assert.Equal(RunSummary.Empty, summary);
        Assert.Equal(new CursorState("w1", null), store.State[Key]);
    }

    [Fact]
    public async Task APinnedRangeWhoseSourceIsNotReady_StaysPending()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { PolledResult.NotReady } };
        var store = new InMemoryCursorStore();
        store.Set(Key, new CursorState("w0", "w1"));

        var summary = await Sweep(source, store).SweepAsync(Handle([], SweepOutcome.Consumed), CancellationToken.None);

        Assert.Equal(RunSummary.Empty, summary);
        Assert.Equal(new CursorRange("w0", "w1"), Assert.Single(source.Ranges));
        Assert.Empty(store.Saves);
        Assert.Equal(new CursorState("w0", "w1"), store.State[Key]);
    }

    [Fact]
    public async Task ASourceThatIsNotReady_SendsNothing_AndSavesNothing()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { PolledResult.NotReady } };
        var store = new InMemoryCursorStore();

        var summary = await Sweep(source, store).SweepAsync(Handle([], SweepOutcome.Consumed), CancellationToken.None);

        Assert.Equal(RunSummary.Empty, summary);
        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task ASourceThatReturnsNull_FailsTheSweep_AndMovesNothing()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { null! } };
        var store = new InMemoryCursorStore();
        store.Set(Key, new CursorState("w0", "w1"));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Sweep(source, store).SweepAsync(Handle([], SweepOutcome.Consumed), CancellationToken.None));

        Assert.Contains("returned null", error.Message, StringComparison.Ordinal);
        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task APortWithAFileAdapterToo_FailsValidation()
    {
        var services = new ServiceCollection();
        services.AddPolledSource(Port, _ => new FakeSource(null));
        services.AddKeyedSingleton(Port, Substitute.For<IFileAdapter>());
        var sweep = new PolledSourcePort(Port).CreateSweep(
            new SourceSweepContext(services.BuildServiceProvider(), "test-component", NullLoggerFactory.Instance));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sweep.ValidateAsync(CancellationToken.None));

        Assert.Contains("file adapter (IFileAdapter) is registered", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReadThatDoesNotMoveTheCursor_IsSentWithAKeyUniqueToTheRun_AndSavesNothing()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0"))
        {
            Reads = { PolledResult.Page([new PolledItem(Bytes("[]"))], null), PolledResult.Page([new PolledItem(Bytes("[]"))], null) }
        };
        var store = new InMemoryCursorStore();
        var handled = new List<SweptItem>();

        await Sweep(source, store).SweepAsync(Handle(handled, SweepOutcome.Consumed), CancellationToken.None);
        await Sweep(source, store).SweepAsync(Handle(handled, SweepOutcome.Consumed), CancellationToken.None);

        Assert.NotEqual(handled[0].Metadata[SourceContextKeys.IdempotencyKey], handled[1].Metadata[SourceContextKeys.IdempotencyKey]);
        Assert.StartsWith("campaigns:w0:w0:", handled[0].Metadata[SourceContextKeys.IdempotencyKey], StringComparison.Ordinal);
        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task ASourceThatCannotPin_KeepsNoPending_AndCommitsAfterHandling()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0", CanPin: false)) { Reads = { PolledResult.Page([new PolledItem(Bytes("[1]"))], "w1") } };
        var store = new InMemoryCursorStore();

        await Sweep(source, store).SweepAsync(Handle([], SweepOutcome.Consumed), CancellationToken.None);

        Assert.Equal([new CursorState("w1", null)], store.Saves);
    }

    [Fact]
    public async Task AFailedRead_FailsTheSweep_AndMovesNothing()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { ReadFailure = new IOException("database unreachable") };
        var store = new InMemoryCursorStore();

        await Assert.ThrowsAsync<IOException>(() =>
            Sweep(source, store).SweepAsync(Handle([], SweepOutcome.Consumed), CancellationToken.None));

        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task AHandlerThatThrows_CountsAFailure_AndKeepsTheRangePending()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { PolledResult.Page([new PolledItem(Bytes("[1]"))], "w1") } };
        var store = new InMemoryCursorStore();

        var summary = await Sweep(source, store).SweepAsync((_, _) => throw new InvalidOperationException("boom"),
            CancellationToken.None);

        Assert.Equal(new RunSummary(0, 1, 0), summary);
        Assert.Equal(new CursorState("w0", "w1"), store.State[Key]);
    }

    [Fact]
    public async Task AHostCancellationWhileHandling_MovesNothing_AndCountsNothing()
    {
        using var cts = new CancellationTokenSource();
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { PolledResult.Page([new PolledItem(Bytes("[1]"))], "w1") } };
        var store = new InMemoryCursorStore();

        var summary = await Sweep(source, store).SweepAsync(async (_, ct) =>
        {
            await cts.CancelAsync();
            ct.ThrowIfCancellationRequested();
            return SweepOutcome.Consumed;
        }, cts.Token);

        Assert.Equal(RunSummary.Empty, summary);
        Assert.Equal(new CursorState("w0", "w1"), store.State[Key]);
    }

    private static ISourceSweep Sweep(FakeSource source, InMemoryCursorStore store)
    {
        var services = new ServiceCollection();
        services.AddPolledSource(Port, _ => source);
        services.AddKeyedSingleton<ICursorStore>(Store, store);
        return new PolledSourcePort(Port).CreateSweep(
            new SourceSweepContext(services.BuildServiceProvider(), "test-component", NullLoggerFactory.Instance));
    }

    private static Func<SweptItem, CancellationToken, Task<SweepOutcome>> Handle(List<SweptItem> handled, SweepOutcome outcome) =>
        (item, _) =>
        {
            handled.Add(item);
            return Task.FromResult(outcome);
        };

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private sealed class FakeSource(CursorOptions? cursor) : IPolledSource
    {
        private int _next;

        public CursorOptions? Cursor => cursor;
        public List<PolledResult> Reads { get; } = [];
        public List<CursorRange> Ranges { get; } = [];
        public Exception? ReadFailure { get; init; }

        public Task<PolledResult> ReadAsync(CursorRange range, CancellationToken ct)
        {
            Ranges.Add(range);
            if (ReadFailure is not null)
                throw ReadFailure;
            return Task.FromResult(Reads[_next++]);
        }
    }

    [Fact]
    public async Task Validate_WithoutASourceForThePort_FailsNamingThePort()
    {
        var sweep = new PolledSourcePort(Port).CreateSweep(
            new SourceSweepContext(new ServiceCollection().BuildServiceProvider(), "test-component", NullLoggerFactory.Instance));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sweep.ValidateAsync(CancellationToken.None));

        Assert.Contains("'campaigns'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_ADeltaWithoutACursorStoreOrADaprClient_FailsNamingTheStore()
    {
        var services = new ServiceCollection();
        services.AddPolledSource(Port, _ => new FakeSource(new CursorOptions(Store, "w0")));
        var sweep = new PolledSourcePort(Port).CreateSweep(
            new SourceSweepContext(services.BuildServiceProvider(), "test-component", NullLoggerFactory.Instance));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sweep.ValidateAsync(CancellationToken.None));

        Assert.Contains("'watermarks'", error.Message, StringComparison.Ordinal);
    }
}
