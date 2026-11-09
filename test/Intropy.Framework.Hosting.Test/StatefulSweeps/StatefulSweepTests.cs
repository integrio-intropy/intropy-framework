using System.Text;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.FileSweeps;
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
    public async Task FullExport_HandsOverTheContent_WithoutAKeyOrAFileName_AndKeepsNoCursor()
    {
        var source = new FakeSource(cursor: null) { Reads = { new PolledRead(Bytes("[1]"), null) } };
        var store = new MemoryStore();
        var handled = new List<SweptFile>();

        var summary = await Sweep(source, store).SweepAsync(Handle(handled, FileOutcome.Consumed), CancellationToken.None);

        Assert.Equal(new RunSummary(1, 0, 0), summary);
        var item = Assert.Single(handled);
        Assert.Equal("[1]", Encoding.UTF8.GetString(await item.ReadAsync()));
        Assert.False(item.IsFile);
        Assert.Equal(Port, item.Port);
        Assert.DoesNotContain(SourceContextKeys.IdempotencyKey, item.Metadata.Keys);
        Assert.Equal(CursorRange.All, Assert.Single(source.Ranges));
        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task Delta_FirstRun_ReadsFromTheInitialCursor_PinsBeforeHandling_AndCommitsAfter()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { new PolledRead(Bytes("[1]"), "w1") } };
        var store = new MemoryStore();
        CursorState? whileHandling = null;

        var summary = await Sweep(source, store).SweepAsync(async (item, _) =>
        {
            whileHandling = await store.GetAsync(Key, CancellationToken.None);
            Assert.Equal("campaigns:w0:w1", item.Metadata[SourceContextKeys.IdempotencyKey]);
            return FileOutcome.Consumed;
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
            Reads = { new PolledRead(Bytes("[1]"), "w1"), new PolledRead(Bytes("[1,2]"), "w2") }
        };
        var store = new MemoryStore();
        var handled = new List<SweptFile>();

        var first = await Sweep(source, store).SweepAsync(Handle(handled, FileOutcome.Failed), CancellationToken.None);
        Assert.Equal(new RunSummary(0, 1, 0), first);
        Assert.Equal(new CursorState("w0", "w1"), store.State[Key]);

        var second = await Sweep(source, store).SweepAsync(Handle(handled, FileOutcome.Consumed), CancellationToken.None);

        Assert.Equal(new RunSummary(1, 0, 0), second);
        // The second read is pinned to the first one's bound, whatever the source would report now.
        Assert.Equal([new CursorRange("w0", null), new CursorRange("w0", "w1")], source.Ranges);
        Assert.Equal(handled[0].Metadata[SourceContextKeys.IdempotencyKey], handled[1].Metadata[SourceContextKeys.IdempotencyKey]);
        Assert.Equal(new CursorState("w1", null), store.State[Key]);
    }

    [Fact]
    public async Task Delta_ADuplicate_IsSkipped_AndCommits()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { new PolledRead(Bytes("[1]"), "w1") } };
        var store = new MemoryStore();
        store.State[Key] = new CursorState("w0", "w1");

        var summary = await Sweep(source, store).SweepAsync(Handle([], FileOutcome.Duplicate), CancellationToken.None);

        Assert.Equal(new RunSummary(0, 0, 1), summary);
        Assert.Equal(new CursorState("w1", null), store.State[Key]);
    }

    [Fact]
    public async Task Delta_WhenCommittingFails_CountsAFailure_AndKeepsTheRangePending()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { new PolledRead(Bytes("[1]"), "w1") } };
        var store = new MemoryStore { FailOn = state => state.Pending is null };

        var summary = await Sweep(source, store).SweepAsync(Handle([], FileOutcome.Consumed), CancellationToken.None);

        Assert.Equal(new RunSummary(0, 1, 0), summary);
        Assert.Equal(new CursorState("w0", "w1"), store.State[Key]);
    }

    [Fact]
    public async Task NothingToSend_SavesNothing()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { null } };
        var store = new MemoryStore();

        var summary = await Sweep(source, store).SweepAsync(Handle([], FileOutcome.Consumed), CancellationToken.None);

        Assert.Equal(RunSummary.Empty, summary);
        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task APinnedRangeWithNothingLeftToSend_IsCommitted_SoItIsNotReadForever()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { null } };
        var store = new MemoryStore();
        store.State[Key] = new CursorState("w0", "w1");

        var summary = await Sweep(source, store).SweepAsync(Handle([], FileOutcome.Consumed), CancellationToken.None);

        Assert.Equal(RunSummary.Empty, summary);
        Assert.Equal(new CursorState("w1", null), store.State[Key]);
    }

    [Fact]
    public async Task AReadThatDoesNotMoveTheCursor_IsSentWithAKeyUniqueToTheRun_AndSavesNothing()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0"))
        {
            Reads = { new PolledRead(Bytes("[]"), null), new PolledRead(Bytes("[]"), null) }
        };
        var store = new MemoryStore();
        var handled = new List<SweptFile>();

        await Sweep(source, store).SweepAsync(Handle(handled, FileOutcome.Consumed), CancellationToken.None);
        await Sweep(source, store).SweepAsync(Handle(handled, FileOutcome.Consumed), CancellationToken.None);

        Assert.NotEqual(handled[0].Metadata[SourceContextKeys.IdempotencyKey], handled[1].Metadata[SourceContextKeys.IdempotencyKey]);
        Assert.StartsWith("campaigns:w0:w0:", handled[0].Metadata[SourceContextKeys.IdempotencyKey], StringComparison.Ordinal);
        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task ASourceThatCannotPin_KeepsNoPending_AndCommitsAfterHandling()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0", CanPin: false)) { Reads = { new PolledRead(Bytes("[1]"), "w1") } };
        var store = new MemoryStore();

        await Sweep(source, store).SweepAsync(Handle([], FileOutcome.Consumed), CancellationToken.None);

        Assert.Equal([new CursorState("w1", null)], store.Saves);
    }

    [Fact]
    public async Task AFailedRead_FailsTheSweep_AndMovesNothing()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { ReadFailure = new IOException("database unreachable") };
        var store = new MemoryStore();

        await Assert.ThrowsAsync<IOException>(() =>
            Sweep(source, store).SweepAsync(Handle([], FileOutcome.Consumed), CancellationToken.None));

        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task AHandlerThatThrows_CountsAFailure_AndKeepsTheRangePending()
    {
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { new PolledRead(Bytes("[1]"), "w1") } };
        var store = new MemoryStore();

        var summary = await Sweep(source, store).SweepAsync((_, _) => throw new InvalidOperationException("boom"),
            CancellationToken.None);

        Assert.Equal(new RunSummary(0, 1, 0), summary);
        Assert.Equal(new CursorState("w0", "w1"), store.State[Key]);
    }

    [Fact]
    public async Task AHostCancellationWhileHandling_MovesNothing_AndCountsNothing()
    {
        using var cts = new CancellationTokenSource();
        var source = new FakeSource(new CursorOptions(Store, "w0")) { Reads = { new PolledRead(Bytes("[1]"), "w1") } };
        var store = new MemoryStore();

        var summary = await Sweep(source, store).SweepAsync(async (_, ct) =>
        {
            await cts.CancelAsync();
            ct.ThrowIfCancellationRequested();
            return FileOutcome.Consumed;
        }, cts.Token);

        Assert.Equal(RunSummary.Empty, summary);
        Assert.Equal(new CursorState("w0", "w1"), store.State[Key]);
    }

    private static StatefulSweep Sweep(FakeSource source, MemoryStore store)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IPolledSource>(Port, source);
        services.AddKeyedSingleton<ICursorStore>(Store, store);
        return new StatefulSweep(services.BuildServiceProvider(), Port, "test-component", NullLoggerFactory.Instance);
    }

    private static Func<SweptFile, CancellationToken, Task<FileOutcome>> Handle(List<SweptFile> handled, FileOutcome outcome) =>
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
        public List<PolledRead?> Reads { get; } = [];
        public List<CursorRange> Ranges { get; } = [];
        public Exception? ReadFailure { get; init; }

        public Task<PolledRead?> ReadAsync(CursorRange range, CancellationToken ct)
        {
            Ranges.Add(range);
            if (ReadFailure is not null)
                throw ReadFailure;
            return Task.FromResult(Reads[_next++]);
        }
    }

    private sealed class MemoryStore : ICursorStore
    {
        public Dictionary<string, CursorState> State { get; } = [];
        public List<CursorState> Saves { get; } = [];
        public Func<CursorState, bool>? FailOn { get; init; }

        public Task<CursorState?> GetAsync(string key, CancellationToken ct) =>
            Task.FromResult(State.GetValueOrDefault(key));

        public Task SaveAsync(string key, CursorState state, CancellationToken ct)
        {
            if (FailOn?.Invoke(state) == true)
                throw new IOException("state store unreachable");
            Saves.Add(state);
            State[key] = state;
            return Task.CompletedTask;
        }
    }
}
