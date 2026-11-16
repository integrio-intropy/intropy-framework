using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Intropy.Framework.Adapters.File;
using Microsoft.Extensions.Configuration;

namespace Intropy.Framework.Hosting.StatefulSweeps;

/// <summary>
/// A source port that is read, not listed (a database query, an HTTP API): each run reads it from
/// its cursor into pages of items. The port's sweep handles the items in order and keeps the
/// cursor, committing it page by page.
/// </summary>
/// <remarks>
/// <para>
/// Register an implementation keyed by the port name, the way a file port registers its
/// <c>IFileAdapter</c> (<c>AddPolledSource</c>), and declare the port as a
/// <see cref="PolledSourcePort"/>: <c>AddPolledSourcePort</c> for a component's single port, a
/// definition's <c>Source</c>, or the sources' <c>Port(new PolledSourcePort(name))</c>. The rest of
/// the component (its routes, its pipelines, the job) is the same as for files.
/// </para>
/// <para>
/// How a source pages its data and what an item is are its own choices, whatever it reads: a query
/// read whole is one page with one item (its rows); read in batches it is several pages; an API's
/// response can be one item, or one item per element; a feed's archive pages are pages, its
/// entries items.
/// </para>
/// <para>
/// The source only reads. It never moves the cursor and never writes to the system it reads: the
/// sweep owns the cursor and moves it only after a page's items have been handled.
/// </para>
/// </remarks>
public interface IPolledSource
{
    /// <summary>The cursor the sweep keeps for this source, or <see langword="null"/> for a source
    /// that reads everything on every run (a full export).</summary>
    CursorOptions? Cursor { get; }

    /// <summary>Reads the source from <paramref name="range"/>.</summary>
    /// <param name="range">What to read. Without a cursor both bounds are <see langword="null"/>.
    /// With one, <see cref="CursorRange.From"/> is the committed cursor (exclusive), and
    /// <see cref="CursorRange.To"/>, when set, is a pinned upper bound (inclusive): a previous run
    /// read up to it and may already have delivered part of it, so read exactly that range again
    /// and nothing beyond it.</param>
    /// <param name="ct">The host's cancellation token.</param>
    /// <returns>What the read found, which decides what happens to the cursor:
    /// <see cref="PolledPages"/> to handle (<see cref="PolledResult.Page"/> for a single page);
    /// <see cref="PolledResult.Empty"/> when the range holds nothing to send; or
    /// <see cref="PolledResult.NotReady"/> when the source cannot tell yet (its data is not
    /// available), so the cursor stays exactly as it is.</returns>
    Task<PolledResult> ReadAsync(CursorRange range, CancellationToken ct);
}

/// <summary>
/// How the sweep keeps a polled source's cursor.
/// </summary>
/// <param name="StoreName">The Dapr state store the cursor is kept in.</param>
/// <param name="Initial">The cursor before the first run, or <see langword="null"/> to read from the
/// beginning.</param>
/// <param name="CanPin">Whether the source can read a bounded range again
/// (<see cref="CursorRange.To"/>). When it can, the sweep saves a page's cursor as pending before the
/// page's items are handled, so a run that fails or crashes part-way through a page is followed by a
/// run that reads exactly the same range, and the idempotency keys absorb what was already delivered.
/// When it cannot (a one-time continuation token), the sweep keeps no pending bound and delivery is
/// plain at-least-once.</param>
/// <remarks>
/// The system's topology mints one state store per component that reads a polled port and sets its
/// name in each such port's section, <c>Ports:&lt;port&gt;:CursorStore</c>: read it with
/// <see cref="FromPort"/> rather than naming the store yourself.
/// </remarks>
public sealed record CursorOptions(string StoreName, string? Initial, bool CanPin = true)
{
    /// <summary>The key in a port's section (<c>Ports:&lt;port&gt;</c>) holding the name of the state
    /// store the port's cursor is kept in. The system's host sets it, from the topology.</summary>
    public const string StoreKey = "CursorStore";

    /// <summary>
    /// The cursor of <paramref name="port"/>, kept in the state store its section names
    /// (<c>Ports:&lt;port&gt;:CursorStore</c>, set by the system's host from the topology).
    /// </summary>
    /// <param name="configuration">The component's configuration.</param>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="initial">The cursor before the first run, or <see langword="null"/> to read from
    /// the beginning.</param>
    /// <param name="canPin">Whether the source can read a bounded range again.</param>
    /// <returns>The cursor options.</returns>
    /// <exception cref="InvalidOperationException">The port's section names no cursor store: the
    /// port is not declared polled in the system's topology, or the component runs outside its
    /// host.</exception>
    public static CursorOptions FromPort(IConfiguration configuration, string port, string? initial = null,
        bool canPin = true)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        var key = $"{PortServiceCollectionExtensions.PortsSectionName}:{port}:{StoreKey}";
        var store = configuration[key];
        if (string.IsNullOrWhiteSpace(store))
            throw new InvalidOperationException(
                $"The polled source port '{port}' has no cursor store: '{key}' is not set. The system's host sets it " +
                "for a component that reads a port the topology declares external (PortRef.DefineExternal); outside the host, set it to the " +
                "Dapr state store the cursor is kept in.");
        return new CursorOptions(store, initial, canPin);
    }
}

/// <summary>
/// The range a polled source is asked to read.
/// </summary>
/// <param name="From">The committed cursor: read what comes after it. <see langword="null"/> on the
/// first run without an initial cursor, and for a source without a cursor.</param>
/// <param name="To">The pinned upper bound, inclusive, or <see langword="null"/> to read everything
/// after <paramref name="From"/> and report the new cursor with each page.</param>
public sealed record CursorRange(string? From, string? To)
{
    /// <summary>The range of a source without a cursor: everything.</summary>
    public static CursorRange All { get; } = new(null, null);
}

/// <summary>
/// What one read of a polled source found. Each outcome has its own cursor behaviour:
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><see cref="PolledPages"/>: pages of items, handled in order. The cursor moves to each page's
/// cursor once the page's items have been handled; the first item that fails stops the run.</item>
/// <item><see cref="Empty"/>: the range holds nothing to send. Nothing is sent; a pinned range is
/// committed, since there is nothing left to deliver from it, and an unpinned read leaves the cursor
/// where it is.</item>
/// <item><see cref="NotReady"/>: the source cannot read the range yet (its data is not available).
/// Nothing is sent and nothing is saved: a pinned range stays pending, to be read again on the next
/// run.</item>
/// </list>
/// </remarks>
public abstract record PolledResult
{
    private protected PolledResult()
    {
    }

    /// <summary>The range holds nothing to send.</summary>
    public static PolledResult Empty { get; } = new EmptyResult();

    /// <summary>The source's data is not available yet; the cursor stays exactly as it is.</summary>
    public static PolledResult NotReady { get; } = new NotReadyResult();

    /// <summary>A read of a single page.</summary>
    /// <param name="items">The page's items, in the order they are handled.</param>
    /// <param name="cursor">The cursor after the page (<see cref="PolledPage.Cursor"/>).</param>
    /// <returns>The read.</returns>
    public static PolledPages Page(IReadOnlyList<PolledItem> items, string? cursor) =>
        new(One(new PolledPage(items, cursor)));

    /// <summary>A read that streams its pages: each page is requested once the previous page's
    /// items have been handled and its cursor committed, so a long read holds one page at a
    /// time.</summary>
    /// <param name="pages">The pages, in order.</param>
    /// <returns>The read.</returns>
    public static PolledPages Pages(IAsyncEnumerable<PolledPage> pages) => new(pages);

    private static async IAsyncEnumerable<PolledPage> One(PolledPage page,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.CompletedTask;
        yield return page;
    }

    private sealed record EmptyResult : PolledResult;

    private sealed record NotReadyResult : PolledResult;
}

/// <summary>
/// A read that found pages of items (<see cref="PolledResult.Page"/>, <see cref="PolledResult.Pages"/>).
/// </summary>
/// <param name="Items">The pages, in order. Enumerated once, by the sweep.</param>
public sealed record PolledPages(IAsyncEnumerable<PolledPage> Items) : PolledResult;

/// <summary>
/// One page of a read: its items and the cursor after it. A page is the unit the cursor moves by:
/// it is committed once all its items have been handled.
/// </summary>
/// <param name="Items">The page's items, in the order they are handled. A page without items still
/// moves the cursor.</param>
/// <param name="Cursor">The cursor after this page, opaque to the sweep (a timestamp, a rowversion,
/// an ETag, a page token). <see langword="null"/> when the page does not move it (an empty result
/// that is still sent) or the source has no cursor. In a pinned read the pinned bound is committed
/// at the end instead.</param>
public sealed record PolledPage(IReadOnlyList<PolledItem> Items, string? Cursor);

/// <summary>
/// One item of a page: a message for the component's pipeline.
/// </summary>
/// <param name="Content">The message's content.</param>
/// <param name="Id">The item's own identity in its source, when it has a stable one (a feed entry's
/// id, a record's key and version): its idempotency key is then <c>{port}:{Id}</c>, which holds
/// however the source pages. Without one the key is the page's range and the item's position in
/// it.</param>
[SuppressMessage("Performance", "CA1819:Properties should not return arrays",
    Justification = "The content is the message payload; byte[] is the shape every pipeline takes.")]
public sealed record PolledItem(byte[] Content, string? Id = null)
{
    /// <summary>Entries for the message's context metadata (such as a row count). The framework's own
    /// keys (the source port, the pickup time, the idempotency key) are set after these and win.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = new Dictionary<string, string>();
}
