using Intropy.Framework.Hosting.Sweeps;

namespace Intropy.Framework.Hosting.StatefulSweeps;

/// <summary>
/// A polled source: a port that is read, not listed (a database query, an HTTP API). Each run reads
/// the port's <see cref="IPolledSource"/> from its cursor into pages of items, handles the items in
/// order, and moves the cursor page by page, only once a page's items have been handled: moving the
/// cursor is what deleting or archiving is for a file.
/// </summary>
/// <remarks>
/// <para>
/// The port's <see cref="IPolledSource"/> is registered separately, keyed by the port name
/// (<c>AddPolledSource</c>, or an adapter package's own registration built on it), the way a file
/// port's adapter is.
/// </para>
/// <para>
/// The sweep owns the cursor protocol; the source only reads. The cursor is kept in an
/// <see cref="ICursorStore"/> (by default the Dapr state store the source names) as one record per
/// component and port, with a <c>committed</c> cursor and a <c>pending</c> upper bound, saved
/// first-write-wins so that a second run of the component fails instead of moving it too:
/// </para>
/// <list type="number">
/// <item>When <c>pending</c> is set, a previous run did not finish the page up to it: read exactly
/// <c>(committed, pending]</c> again, and commit <c>pending</c> with that read's last item.</item>
/// <item>Otherwise read everything after <c>committed</c>. For each page that moves the cursor, save
/// its cursor as <c>pending</c>, hand its items over in order, each with its idempotency key in the
/// context metadata (<see cref="Blocks.Shared.SourceContextKeys.IdempotencyKey"/>), and with the
/// page's last item save <c>committed = pending</c> and clear <c>pending</c>.</item>
/// <item>The first item that fails stops the read: its page stays pending, and no later item is
/// handled. A business incident is not a failure.</item>
/// </list>
/// <para>
/// An item's idempotency key is <c>{port}:{id}</c> when the source gives it an id; otherwise it is
/// the page's range, <c>{port}:{committed}:{pending}</c>, followed by <c>#{position}</c> unless the
/// page is a single item. Pinning the page's bound before handing its items over is what makes a
/// re-read after a crash produce the same keys: were the bound recomputed, new data would move it,
/// and the overlap would be sent twice. A source that cannot re-read a bounded range
/// (<see cref="CursorOptions.CanPin"/> false) keeps no <c>pending</c> and gets plain at-least-once
/// delivery. A pinned range the source reports <see cref="PolledResult.Empty"/> is committed, so it
/// is not read again on every run; one it reports <see cref="PolledResult.NotReady"/> stays pending.
/// </para>
/// <para>
/// A source without a cursor (a full export) reads everything on every run: there is nothing to
/// commit, and an item has an idempotency key only when it has an id. A page that does not move the
/// cursor (an empty result the source still sends) saves nothing, and its items without an id get a
/// key unique to the run, so they are never taken for duplicates.
/// </para>
/// <para>
/// Every other rule is a framework sweep's (<see cref="SourceSweep{TEntry, TItem}"/>): each item is
/// handled in its own scope and trace, an interrupted item never moves the cursor, and committing is
/// always awaited.
/// </para>
/// </remarks>
/// <param name="Name">The port name, as declared in the system's topology; the key of the port's
/// <see cref="IPolledSource"/>.</param>
public sealed record PolledSourcePort(string Name) : SourcePort(Name)
{
    /// <inheritdoc />
    public override ISourceSweep CreateSweep(SourceSweepContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new StatefulSweep(context, Name);
    }
}
