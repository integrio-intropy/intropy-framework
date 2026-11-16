using System.Runtime.CompilerServices;
using System.Text;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.StatefulSweeps;
using Intropy.Framework.Hosting.Sweeps;
using Intropy.Framework.Testing.StatefulSweeps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Test.StatefulSweeps;

/// <summary>
/// A polled read of several pages and items: pages are requested one at a time, each pinned as
/// pending before its items are handled and committed with its last item; items are handled in
/// order and the first failure stops the read, leaving its page pending; a pinned re-read of a page
/// gives its items the same keys; an item with an id is keyed on it; and a second run that moved the
/// cursor makes this run's save fail instead of overwriting it.
/// </summary>
public class PagedReadTests
{
    private const string Port = "orders";
    private const string Store = "cursors";
    private const string Key = "test-component:orders";

    [Fact]
    public async Task EachPage_IsPinnedBeforeItsItems_AndCommittedWithItsLastItem_BeforeTheNextPageIsRead()
    {
        var store = new InMemoryCursorStore();
        var source = new PagedSource(store, Page("c1", "a", "b"), Page("c2", "c"));
        var handled = new List<(string Item, CursorState? Cursor)>();

        var summary = await Sweep(source, store).SweepAsync(async (item, _) =>
        {
            handled.Add((Text(await item.ReadAsync(CancellationToken.None)), (await store.GetAsync(Key, CancellationToken.None))?.State));
            return SweepOutcome.Consumed;
        }, CancellationToken.None);

        Assert.Equal(new RunSummary(3, 0, 0), summary);
        Assert.Equal(
            [("a", new CursorState("c0", "c1")), ("b", new CursorState("c0", "c1")), ("c", new CursorState("c1", "c2"))],
            handled);
        // The second page was requested only once the first was committed.
        Assert.Equal([null, new CursorState("c1", null)], source.CursorWhenPageRequested);
        Assert.Equal(
            [new CursorState("c0", "c1"), new CursorState("c1", null), new CursorState("c1", "c2"), new CursorState("c2", null)],
            store.Saves);
    }

    [Fact]
    public async Task ItemsOfAPage_AreKeyedOnTheRangeAndTheirPosition_ASingleItemOnTheRangeAlone()
    {
        var store = new InMemoryCursorStore();
        var handled = new List<SweptItem>();

        await Sweep(new PagedSource(store, Page("c1", "a", "b"), Page("c2", "c")), store)
            .SweepAsync(Handle(handled), CancellationToken.None);

        Assert.Equal(["orders:c0:c1#1", "orders:c0:c1#2", "orders:c1:c2"],
            handled.Select(i => i.Metadata[SourceContextKeys.IdempotencyKey]));
    }

    [Fact]
    public async Task AnItemThatFails_StopsTheRead_LeavesItsPagePending_AndKeepsTheCommittedPages()
    {
        var store = new InMemoryCursorStore();
        var source = new PagedSource(store, Page("c1", "a"), Page("c2", "b", "c", "d"), Page("c3", "e"));
        var handled = new List<string>();

        var summary = await Sweep(source, store).SweepAsync(async (item, _) =>
        {
            var text = Text(await item.ReadAsync(CancellationToken.None));
            handled.Add(text);
            return text == "c" ? SweepOutcome.Failed : SweepOutcome.Consumed;
        }, CancellationToken.None);

        Assert.Equal(new RunSummary(2, 1, 0), summary);
        Assert.Equal(["a", "b", "c"], handled);
        Assert.Equal(new CursorState("c1", "c2"), store.State[Key]);
        Assert.Equal(2, source.CursorWhenPageRequested.Count);
    }

    [Fact]
    public async Task ABusinessIncidentOrADuplicate_DoesNotStopTheRead()
    {
        var store = new InMemoryCursorStore();
        var outcomes = new Queue<SweepOutcome>([SweepOutcome.Duplicate, SweepOutcome.Consumed, SweepOutcome.Consumed]);

        var summary = await Sweep(new PagedSource(store, Page("c1", "a", "b"), Page("c2", "c")), store)
            .SweepAsync((_, _) => Task.FromResult(outcomes.Dequeue()), CancellationToken.None);

        Assert.Equal(new RunSummary(2, 0, 1), summary);
        Assert.Equal(new CursorState("c2", null), store.State[Key]);
    }

    [Fact]
    public async Task APinnedReRead_GivesThePagesItemsTheSameKeys_AndCommitsThePendingBoundWithItsLastItem()
    {
        var store = new InMemoryCursorStore();
        var first = new List<SweptItem>();
        await Sweep(new PagedSource(store, Page("c1", "a", "b")), store).SweepAsync(async (item, _) =>
        {
            first.Add(item);
            return Text(await item.ReadAsync(CancellationToken.None)) == "b" ? SweepOutcome.Failed : SweepOutcome.Consumed;
        }, CancellationToken.None);

        var source = new PagedSource(store, Page("ignored", "a", "b"));
        var again = new List<SweptItem>();
        var summary = await Sweep(source, store).SweepAsync(Handle(again), CancellationToken.None);

        Assert.Equal(new CursorRange("c0", "c1"), Assert.Single(source.Ranges));
        Assert.Equal(new RunSummary(2, 0, 0), summary);
        Assert.Equal(first.Select(Key_), again.Select(Key_));
        Assert.Equal(new CursorState("c1", null), store.State[Key]);
    }

    [Fact]
    public async Task APinnedReRead_InSeveralPages_CommitsThePendingBoundOnlyAfterItsLastPage()
    {
        var store = new InMemoryCursorStore();
        store.Set(Key, new CursorState("c0", "c2"));
        var states = new List<CursorState?>();

        await Sweep(new PagedSource(store, Page("c1", "a"), Page("c2", "b")), store).SweepAsync(async (_, _) =>
        {
            states.Add((await store.GetAsync(Key, CancellationToken.None))?.State);
            return SweepOutcome.Consumed;
        }, CancellationToken.None);

        Assert.Equal([new CursorState("c0", "c2"), new CursorState("c0", "c2")], states);
        Assert.Equal([new CursorState("c2", null)], store.Saves);
    }

    [Fact]
    public async Task AnItemWithAnId_IsKeyedOnIt_AndTracedAsIt()
    {
        var store = new InMemoryCursorStore();
        var handled = new List<SweptItem>();
        var page = new PolledPage([new PolledItem(Bytes("a"), "entry-1"), new PolledItem(Bytes("b"), "entry-2")], "c1");

        await Sweep(new PagedSource(store, page), store).SweepAsync(Handle(handled), CancellationToken.None);

        Assert.Equal(["orders:entry-1", "orders:entry-2"], handled.Select(i => i.Metadata[SourceContextKeys.IdempotencyKey]));
        Assert.Equal(["entry-1", "entry-2"], handled.Select(i => i.Id));
    }

    [Fact]
    public async Task APageWithoutItems_MovesTheCursor_WithoutPinningIt()
    {
        var store = new InMemoryCursorStore();
        var handled = new List<SweptItem>();

        await Sweep(new PagedSource(store, new PolledPage([], "c1"), Page("c2", "a")), store)
            .SweepAsync(Handle(handled), CancellationToken.None);

        Assert.Single(handled);
        Assert.Equal(
            [new CursorState("c1", null), new CursorState("c1", "c2"), new CursorState("c2", null)],
            store.Saves);
    }

    [Fact]
    public async Task ACursorMovedByAnotherRun_FailsThisRunsCommit_AndIsNotOverwritten()
    {
        var store = new InMemoryCursorStore();
        var source = new PagedSource(store, Page("c1", "a"), Page("c2", "b"));

        var summary = await Sweep(source, store).SweepAsync((_, _) =>
        {
            // Another run moves the cursor while this one handles its item.
            store.Set(Key, new CursorState("c9", null));
            return Task.FromResult(SweepOutcome.Consumed);
        }, CancellationToken.None);

        Assert.Equal(new RunSummary(0, 1, 0), summary);
        Assert.Equal(new CursorState("c9", null), store.State[Key]);
        Assert.Single(source.CursorWhenPageRequested);
    }

    private static PolledPage Page(string cursor, params string[] items) =>
        new(items.Select(i => new PolledItem(Bytes(i))).ToList(), cursor);

    private static string Key_(SweptItem item) => item.Metadata[SourceContextKeys.IdempotencyKey];

    private static ISourceSweep Sweep(IPolledSource source, InMemoryCursorStore store)
    {
        var services = new ServiceCollection();
        services.AddPolledSource(Port, _ => source);
        services.AddKeyedSingleton<ICursorStore>(Store, store);
        return new PolledSourcePort(Port).CreateSweep(
            new SourceSweepContext(services.BuildServiceProvider(), "test-component", NullLoggerFactory.Instance));
    }

    private static Func<SweptItem, CancellationToken, Task<SweepOutcome>> Handle(List<SweptItem> handled) =>
        (item, _) =>
        {
            handled.Add(item);
            return Task.FromResult(SweepOutcome.Consumed);
        };

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(byte[] bytes) => Encoding.UTF8.GetString(bytes);

    /// <summary>Streams its pages, recording the committed cursor when each is requested.</summary>
    private sealed class PagedSource(InMemoryCursorStore store, params PolledPage[] pages) : IPolledSource
    {
        public CursorOptions? Cursor { get; } = new(Store, "c0");
        public List<CursorRange> Ranges { get; } = [];
        public List<CursorState?> CursorWhenPageRequested { get; } = [];

        public Task<PolledResult> ReadAsync(CursorRange range, CancellationToken ct)
        {
            Ranges.Add(range);
            return Task.FromResult<PolledResult>(PolledResult.Pages(Stream(ct)));
        }

        private async IAsyncEnumerable<PolledPage> Stream([EnumeratorCancellation] CancellationToken ct)
        {
            foreach (var page in pages)
            {
                CursorWhenPageRequested.Add(store.State.GetValueOrDefault(Key) is { Pending: null } committed ? committed : null);
                await Task.Yield();
                yield return page;
            }
        }
    }
}
