using Acme.Feeds;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.StatefulSweeps;
using Intropy.Framework.Hosting.Sweeps;
using Intropy.Framework.Testing.StatefulSweeps;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Extensibility.Test;

/// <summary>
/// A special case the framework does not ship, built on its public mechanics: an archived feed read
/// as a polled source (<see cref="ArchivedFeedSource"/>). The framework's sweep gives it what a feed
/// reader needs: a backfill and a resume across a page that was archived since, one item per entry,
/// a cursor committed page by page, a stop at the first failed entry and an exact re-read of its page,
/// and a failed run when the cursor's entry is gone.
/// </summary>
public class ArchivedFeedTests
{
    private const string Port = "feed";
    private const string Store = "cursors";
    private const string Key = "feed-reader:feed";

    private readonly ArchivedFeed _feed = new(pageSize: 3);
    private readonly InMemoryCursorStore _store = new();

    [Fact]
    public async Task WithoutACursor_EveryEntryIsRead_OldestFirst_AndTheCursorCommittedPageByPage()
    {
        _feed.Append("e1", "e2", "e3", "e4", "e5", "e6", "e7");
        var handled = new List<string>();

        var summary = await SweepAsync(RecordIds(handled));

        Assert.Equal(new RunSummary(7, 0, 0), summary);
        Assert.Equal(["e1", "e2", "e3", "e4", "e5", "e6", "e7"], handled);
        Assert.Equal(
            ["1/e3", "2/e6", "3/e7"],
            _store.Saves.Where(s => s.Pending is null).Select(s => s.Committed));
    }

    [Fact]
    public async Task ARun_ResumesAfterItsEntry_ThroughAPageThatWasArchivedSince()
    {
        _feed.Append("e1", "e2", "e3", "e4");
        await SweepAsync(RecordIds([]));

        // Page 2 fills up and is archived; page 3 is the recent page now.
        _feed.Append("e5", "e6", "e7");
        var handled = new List<string>();
        await SweepAsync(RecordIds(handled));

        Assert.Equal(["e5", "e6", "e7"], handled);
        Assert.Equal(new CursorState("3/e7", null), _store.State[Key]);
    }

    [Fact]
    public async Task AFailedEntry_StopsTheRun_AndTheNextRunReadsExactlyItsPageAgain_KeyedOnTheEntries()
    {
        _feed.Append("e1", "e2", "e3", "e4", "e5", "e6");
        var first = new List<SweptItem>();
        var failed = await SweepAsync((item, _) =>
        {
            first.Add(item);
            return Task.FromResult(item.Id == "e5" ? SweepOutcome.Failed : SweepOutcome.Consumed);
        });

        Assert.Equal(new RunSummary(4, 1, 0), failed);
        Assert.Equal(new CursorState("1/e3", "2/e6"), _store.State[Key]);

        // New entries arrive; the re-read stops at the page it pinned.
        _feed.Append("e7");
        var again = new List<SweptItem>();
        await SweepAsync(RecordItems(again));
        Assert.Equal(["e4", "e5", "e6"], again.Select(i => i.Id));
        Assert.Equal(
            first.Where(i => i.Id is "e4" or "e5").Select(i => i.Metadata[SourceContextKeys.IdempotencyKey]),
            again.Take(2).Select(i => i.Metadata[SourceContextKeys.IdempotencyKey]));
        Assert.Equal(new CursorState("2/e6", null), _store.State[Key]);

        var next = new List<SweptItem>();
        await SweepAsync(RecordItems(next));
        Assert.Equal(["e7"], next.Select(i => i.Id));
    }

    [Fact]
    public async Task ACursorWhoseEntryIsGone_FailsTheRun_AndMovesNothing()
    {
        _feed.Append("e1", "e2");
        _store.Set(Key, new CursorState("1/e9", null));

        await Assert.ThrowsAsync<InvalidOperationException>(() => SweepAsync(RecordIds([])));

        Assert.Empty(_store.Saves);
    }

    private static Func<SweptItem, CancellationToken, Task<SweepOutcome>> RecordIds(List<string> handled) =>
        (item, _) =>
        {
            handled.Add(item.Id);
            return Task.FromResult(SweepOutcome.Consumed);
        };

    private static Func<SweptItem, CancellationToken, Task<SweepOutcome>> RecordItems(List<SweptItem> handled) =>
        (item, _) =>
        {
            handled.Add(item);
            return Task.FromResult(SweepOutcome.Consumed);
        };

    private Task<RunSummary> SweepAsync(Func<SweptItem, CancellationToken, Task<SweepOutcome>> handle)
    {
        var services = new ServiceCollection();
        // As the system's host sets it: Ports__feed__CursorStore.
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            [$"Ports:{Port}:{CursorOptions.StoreKey}"] = Store,
        }).Build();
        services.AddPolledSource(Port, _ => new ArchivedFeedSource(_feed, configuration, Port));
        services.AddKeyedSingleton<ICursorStore>(Store, _store);
        var sweep = new PolledSourcePort(Port).CreateSweep(
            new SourceSweepContext(services.BuildServiceProvider(), "feed-reader", NullLoggerFactory.Instance));
        return sweep.SweepAsync(handle, CancellationToken.None);
    }
}
