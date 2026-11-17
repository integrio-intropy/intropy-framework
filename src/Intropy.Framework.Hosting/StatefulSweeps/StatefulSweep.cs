using System.Runtime.CompilerServices;
using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Sweeps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.StatefulSweeps;

/// <summary>
/// One run's sweep of a <see cref="PolledSourcePort"/>: reads the port's <see cref="IPolledSource"/>
/// from its cursor, handles each page's items in order, and keeps the cursor with the
/// committed/pending protocol the port describes. Listing is the read (and pinning <c>pending</c>
/// before each page); completing a page's last item is the commit.
/// </summary>
internal sealed class StatefulSweep : SourceSweep<StatefulSweep.Entry, StatefulSweep.Item>
{
    private readonly ILogger _logger;
    private ICursorStore? _store;
    private string? _etag;
    private bool _stopped;

    internal StatefulSweep(SourceSweepContext context, string port) : base(context, port) =>
        _logger = context.LoggerFactory.CreateLogger<StatefulSweep>();

    /// <summary>The cursor record's key: one per component and port.</summary>
    private string CursorKey => $"{Context.ComponentName}:{Port}";

    /// <summary>Checks that the port's source is registered, that no file adapter is, and, for a source
    /// with a cursor, that its cursor store can be reached from the component.</summary>
    /// <exception cref="InvalidOperationException">No source is registered for the port, a file
    /// adapter is, or its cursor store cannot be resolved.</exception>
    public override Task ValidateAsync(CancellationToken ct)
    {
        var source = ResolveSource();
        if (Context.Services.GetKeyedService<IFileAdapter>(Port) is not null)
            throw new InvalidOperationException(
                $"Component '{Context.ComponentName}': source port '{Port}' is declared as a polled source port, but a " +
                "file adapter (IFileAdapter) is registered for it. A port has either a file adapter or a polled source.");
        if (source.Cursor is { } cursor)
            _ = ResolveStore(cursor.StoreName);
        return Task.CompletedTask;
    }

    /// <summary>Reads the source once and yields its pages' items, in order: nothing when the source
    /// is empty or not ready (<see cref="PolledResult"/>), and nothing more after an item fails.</summary>
    protected override async IAsyncEnumerable<Entry> ListAsync([EnumeratorCancellation] CancellationToken ct)
    {
        var source = ResolveSource();
        var cursor = source.Cursor;

        var state = new CursorState(null, null);
        if (cursor is not null)
        {
            _store = ResolveStore(cursor.StoreName);
            var stored = await _store.GetAsync(CursorKey, ct);
            state = stored?.State ?? new CursorState(cursor.Initial, null);
            _etag = stored?.ETag;
        }

        var pinned = cursor is { CanPin: true } && state.Pending is not null;
        var range = cursor is null ? CursorRange.All : new CursorRange(state.Committed, pinned ? state.Pending : null);
        if (pinned)
            _logger.LogInformation(
                "Source port {SourcePort} has a pending read that was not committed; reading exactly ({From}, {To}] again",
                Port, range.From, range.To);

        var result = await source.ReadAsync(range, ct) ?? throw new InvalidOperationException(
            $"Component '{Context.ComponentName}': the source of port '{Port}' returned null; return PolledResult.Page, " +
            "PolledResult.Pages, PolledResult.Empty or PolledResult.NotReady.");
        HostingMetrics.RecordPolledRead(Context.ComponentName, Port, result switch
        {
            PolledPages => "pages",
            _ when ReferenceEquals(result, PolledResult.NotReady) => "not_ready",
            _ => "empty"
        });
        if (result is not PolledPages read)
        {
            if (ReferenceEquals(result, PolledResult.NotReady))
                _logger.LogInformation("The data of source port {SourcePort} of {Component} is not available yet; " +
                                       "nothing is sent and the cursor stays", Port, Context.ComponentName);
            else if (pinned)
                await CommitPinnedAsync(state, "has nothing to send any more");
            else
                _logger.LogInformation("Source port {SourcePort} of {Component} has nothing to send", Port, Context.ComponentName);
            yield break;
        }

        var entries = pinned ? PinnedAsync(read, state, ct) : UnpinnedAsync(read, cursor, state, ct);
        await foreach (var entry in entries)
            yield return entry;
    }

    /// <summary>A pinned re-read: everything it yields covers <c>(committed, pending]</c>, and the
    /// pending bound is committed with its last item (or at its end, when that has no item).</summary>
    private async IAsyncEnumerable<Entry> PinnedAsync(PolledPages read, CursorState state,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var (from, to) = (state.Committed, state.Pending!);
        var position = 0;
        await using var pages = read.Items.GetAsyncEnumerator(ct);
        var more = await pages.MoveNextAsync();
        if (!more)
        {
            await CommitPinnedAsync(state, "yields no page any more");
            yield break;
        }

        while (more)
        {
            var page = pages.Current;
            // Read ahead: the pending bound is committed with the read's last item.
            more = await pages.MoveNextAsync();
            if (page.Items.Count == 0)
            {
                if (!more)
                    await CommitPinnedAsync(state, "ends without an item to send");
                continue;
            }

            for (var i = 0; i < page.Items.Count; i++)
            {
                if (_stopped)
                    yield break;
                position++;
                var last = !more && i == page.Items.Count - 1;
                yield return Describe(page.Items[i], position, page.Items.Count, from, to, pinned: true,
                    commit: last ? to : null);
            }
        }
    }

    /// <summary>A read from the committed cursor: each page that moves the cursor is pinned as
    /// pending before its items are handled, and committed with its last item.</summary>
    private async IAsyncEnumerable<Entry> UnpinnedAsync(PolledPages read, CursorOptions? cursor, CursorState state,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var committed = state.Committed;
        await foreach (var page in read.Items.WithCancellation(ct))
        {
            var to = cursor is null ? null : page.Cursor;
            var moves = cursor is not null && to is not null && !string.Equals(to, committed, StringComparison.Ordinal);
            if (moves && page.Items.Count == 0)
            {
                // Nothing to handle: the page only moves the cursor.
                await SaveAsync(new CursorState(to, null), ct);
                committed = to;
                continue;
            }

            if (moves && cursor!.CanPin)
                await SaveAsync(new CursorState(committed, to), ct);

            for (var i = 0; i < page.Items.Count; i++)
            {
                if (_stopped)
                    yield break;
                var last = i == page.Items.Count - 1;
                yield return moves
                    ? Describe(page.Items[i], i + 1, page.Items.Count, committed, to, pinned: false, commit: last ? to : null)
                    : DescribeUnmoved(page.Items[i], i + 1, page.Items.Count, committed, cursor is not null);
            }

            // The last item's outcome is known here: after a failure the page stays pending.
            if (_stopped)
                yield break;
            if (moves)
                committed = to;
        }
    }

    /// <summary>An item of a page that moves the cursor, or of a pinned read.</summary>
    private Entry Describe(PolledItem item, int position, int count, string? from, string? to, bool pinned, string? commit)
    {
        var suffix = position == 1 && count == 1 ? "" : $"#{position}";
        var metadata = new Dictionary<string, string>(item.Metadata, StringComparer.Ordinal)
        {
            [SourceContextKeys.IdempotencyKey] = item.Id is { } id ? $"{Port}:{id}" : $"{Port}:{from}:{to}{suffix}"
        };
        var name = item.Id ?? $"{Port} ({from}, {to}]{suffix}";
        return new Entry(name, item.Content, metadata, from, to, pinned, commit);
    }

    /// <summary>An item of a page that does not move the cursor (or of a source without one).</summary>
    private Entry DescribeUnmoved(PolledItem item, int position, int count, string? committed, bool hasCursor)
    {
        var metadata = new Dictionary<string, string>(item.Metadata, StringComparer.Ordinal);
        if (item.Id is { } id)
            metadata[SourceContextKeys.IdempotencyKey] = $"{Port}:{id}";
        else if (hasCursor)
            // Nothing new, so nothing to deduplicate: unique to this run.
            metadata[SourceContextKeys.IdempotencyKey] = $"{Port}:{committed}:{committed}:{Guid.NewGuid():N}";

        var suffix = position == 1 && count == 1 ? "" : $" #{position}";
        var name = item.Id ?? (hasCursor ? $"{Port} ({committed}, {committed}]{suffix}" : $"{Port}{suffix}");
        return new Entry(name, item.Content, metadata, hasCursor ? committed : null, hasCursor ? committed : null,
            Pinned: false, CommitTo: null);
    }

    private async Task CommitPinnedAsync(CursorState state, string reason)
    {
        // The pinned range has nothing left to deliver: keeping it pending would read it on every run.
        await SaveAsync(new CursorState(state.Pending, null), CancellationToken.None);
        _logger.LogInformation("The pending read of {SourcePort} {Reason}; committed its bound {To}",
            Port, reason, state.Pending);
    }

    private async Task SaveAsync(CursorState state, CancellationToken ct) =>
        _etag = await _store!.SaveAsync(CursorKey, state, _etag, ct);

    /// <inheritdoc />
    protected override string IdOf(Entry entry) => entry.Id;

    /// <inheritdoc />
    protected override Item CreateItem(Entry entry, IServiceProvider scope) => new(entry, Port, scope);

    /// <summary>Commits the cursor with a page's last item: <c>committed = pending</c>, <c>pending</c>
    /// cleared. Every other item has nothing to complete.</summary>
    protected override async Task CompleteAsync(Item item, SweepOutcome outcome)
    {
        if (item.Entry.CommitTo is not { } to)
            return;

        // Committing finishes an already handled page and is always awaited. A failure leaves the
        // page pending: the next run reads it again and the idempotency keys absorb it.
        await SaveAsync(new CursorState(to, null), CancellationToken.None);
        _logger.LogInformation("Committed the cursor of source port {SourcePort} at {Cursor}", Port, to);
    }

    /// <summary>The range on the item's span.</summary>
    protected override SweepItemTelemetry TelemetryOf(Entry entry)
    {
        var tags = new Dictionary<string, object?>();
        if (entry.From is not null)
            tags["intropy.cursor.from"] = entry.From;
        if (entry.To is not null)
            tags["intropy.cursor.to"] = entry.To;
        if (entry.Pinned)
            tags["intropy.cursor.pinned"] = true;
        return new SweepItemTelemetry { SpanTags = tags };
    }

    /// <summary>A failed item stops the read, since a polled source's items are ordered and the
    /// cursor must not pass it.</summary>
    protected override void OnItemSwept(Entry entry, SweepOutcome outcome)
    {
        if (outcome is SweepOutcome.Failed)
        {
            _stopped = true;
            _logger.LogWarning("{SourceItem} of source port {SourcePort} failed; the read stops before the next item, " +
                               "and the next run reads its page again", entry.Id, Port);
        }
    }

    private IPolledSource ResolveSource() =>
        Context.Services.GetKeyedService<IPolledSource>(Port) ?? throw new InvalidOperationException(
            $"Component '{Context.ComponentName}': no IPolledSource is registered for the source port '{Port}'.");

    private ICursorStore ResolveStore(string storeName) =>
        Context.Services.GetKeyedService<ICursorStore>(storeName)
        ?? new DaprCursorStore(Context.Services.GetService<DaprClient>() ?? throw new InvalidOperationException(
                $"Component '{Context.ComponentName}': source port '{Port}' keeps its cursor in the state store " +
                $"'{storeName}', but no DaprClient is registered."),
            storeName);

    /// <summary>One item of a read, with the range it belongs to.</summary>
    /// <param name="Id">The item's identity: its own id, or the port, the range and its position.</param>
    /// <param name="Content">The message's content.</param>
    /// <param name="Metadata">The source's metadata, with the idempotency key.</param>
    /// <param name="From">The committed cursor the page started after.</param>
    /// <param name="To">The cursor after the page, or the pinned bound.</param>
    /// <param name="Pinned">Whether the item came from a pinned re-read.</param>
    /// <param name="CommitTo">The cursor to commit once this item is handled: set on a page's last
    /// item when the page moves the cursor.</param>
    internal sealed record Entry(
        string Id,
        byte[] Content,
        IReadOnlyDictionary<string, string> Metadata,
        string? From,
        string? To,
        bool Pinned,
        string? CommitTo);

    /// <summary>An item, handed to the component inside its own scope.</summary>
    internal sealed class Item(Entry entry, string port, IServiceProvider services)
        : SweptItem(entry.Id, port, services)
    {
        internal Entry Entry { get; } = entry;

        /// <inheritdoc />
        public override IReadOnlyDictionary<string, string> Metadata => Entry.Metadata;

        /// <inheritdoc />
        public override Task<byte[]> ReadAsync(CancellationToken ct = default) => Task.FromResult(Entry.Content);
    }
}
