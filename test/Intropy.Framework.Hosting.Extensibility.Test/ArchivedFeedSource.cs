using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Intropy.Framework.Hosting.StatefulSweeps;
using Microsoft.Extensions.Configuration;

namespace Acme.Feeds;

/// <summary>
/// A feed paged like an archived Atom feed (RFC 5005): a recent page that fills up and is then
/// archived under a permanent page id, archive pages that never change, and links between them. In
/// memory here: what a component would read over HTTP.
/// </summary>
public sealed class ArchivedFeed(int pageSize)
{
    private readonly List<List<FeedEntry>> _pages = [[]];

    /// <summary>How many pages were fetched.</summary>
    public int Fetches { get; private set; }

    public ArchivedFeed Append(params string[] ids)
    {
        foreach (var id in ids)
        {
            if (_pages[^1].Count == pageSize)
                _pages.Add([]);
            _pages[^1].Add(new FeedEntry(id, $"<entry>{id}</entry>"));
        }

        return this;
    }

    /// <summary>The recent page: the newest, still filling up.</summary>
    public FeedPage Recent() => Get(_pages.Count);

    /// <summary>A page by its permanent id (1 is the oldest).</summary>
    public FeedPage Get(int pageId)
    {
        Fetches++;
        if (pageId < 1 || pageId > _pages.Count)
            throw new InvalidOperationException($"No page {pageId}");
        return new FeedPage(pageId,
            pageId > 1 ? pageId - 1 : null,
            pageId < _pages.Count ? pageId + 1 : null,
            [.. _pages[pageId - 1]]);
    }
}

public sealed record FeedPage(int Id, int? PrevArchive, int? NextArchive, IReadOnlyList<FeedEntry> Entries);

public sealed record FeedEntry(string Id, string Xml);

/// <summary>
/// The feed as a polled source, built by a component on the framework's public mechanics only: the
/// cursor is <c>{page id}/{last entry id}</c>; a read starts after that entry on that page and walks
/// the archive links forward to the recent page, one polled page per feed page, one item per entry,
/// keyed on the entry's id. A pinned read stops at the pinned entry. An entry missing from its page
/// fails the read: it never guesses where to resume.
/// </summary>
public sealed class ArchivedFeedSource(ArchivedFeed feed, IConfiguration configuration, string port) : IPolledSource
{
    // The store is the one the system's host names for the port, from the topology.
    public CursorOptions? Cursor { get; } = CursorOptions.FromPort(configuration, port);

    public Task<PolledResult> ReadAsync(CursorRange range, CancellationToken ct) =>
        Task.FromResult<PolledResult>(PolledResult.Pages(WalkAsync(range, ct)));

    private async IAsyncEnumerable<PolledPage> WalkAsync(CursorRange range, [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.Yield();
        var to = range.To is null ? ((int Page, string Entry)?)null : Parse(range.To);
        var (page, after) = range.From is null ? (Oldest(), (string?)null) : Start(Parse(range.From));
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var last = to is { } pin && pin.Page == page.Id;
            var first = after is null ? 0 : IndexOf(page, after) + 1;
            var end = last ? IndexOf(page, to!.Value.Entry) + 1 : page.Entries.Count;
            var entries = page.Entries.Skip(first).Take(end - first).ToList();

            if (entries.Count > 0)
                yield return new PolledPage(
                    entries.Select(e => new PolledItem(Encoding.UTF8.GetBytes(e.Xml), e.Id)).ToList(),
                    $"{page.Id.ToString(CultureInfo.InvariantCulture)}/{entries[^1].Id}");

            if (last || page.NextArchive is not { } next)
                yield break;
            (page, after) = (feed.Get(next), null);
        }
    }

    private FeedPage Oldest()
    {
        var page = feed.Recent();
        while (page.PrevArchive is { } previous)
            page = feed.Get(previous);
        return page;
    }

    private (FeedPage Page, string After) Start((int Page, string Entry) cursor) => (feed.Get(cursor.Page), cursor.Entry);

    private static int IndexOf(FeedPage page, string entry)
    {
        var index = page.Entries.ToList().FindIndex(e => e.Id == entry);
        return index >= 0
            ? index
            : throw new InvalidOperationException($"Page {page.Id} has no entry '{entry}': the cursor was not found");
    }

    private static (int Page, string Entry) Parse(string cursor)
    {
        var slash = cursor.IndexOf('/', StringComparison.Ordinal);
        return (int.Parse(cursor[..slash], CultureInfo.InvariantCulture), cursor[(slash + 1)..]);
    }
}
