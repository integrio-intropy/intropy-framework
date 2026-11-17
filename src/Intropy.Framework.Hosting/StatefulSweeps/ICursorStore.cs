using Dapr.Client;

namespace Intropy.Framework.Hosting.StatefulSweeps;

/// <summary>
/// Where a <see cref="PolledSourcePort"/>'s sweep keeps a polled source's cursor: one record per
/// component and port, saved first-write-wins so that two runs never both move it.
/// </summary>
/// <remarks>
/// The default is a Dapr state store (<see cref="DaprCursorStore"/>), named by the source's
/// <see cref="CursorOptions.StoreName"/>. Register an implementation keyed by that store name to keep
/// cursors somewhere else.
/// </remarks>
public interface ICursorStore
{
    /// <summary>Reads a cursor record and its version.</summary>
    /// <param name="key">The record's key.</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>The record and its ETag, or <see langword="null"/> when none is saved.</returns>
    Task<StoredCursor?> GetAsync(string key, CancellationToken ct);

    /// <summary>Saves a cursor record if it has not changed since <paramref name="etag"/> was read.</summary>
    /// <param name="key">The record's key.</param>
    /// <param name="state">The record.</param>
    /// <param name="etag">The ETag the record was read or last saved with; <see langword="null"/> when
    /// there was none.</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>The record's new ETag.</returns>
    /// <exception cref="CursorConflictException">The record changed since: another run moved the
    /// cursor.</exception>
    Task<string> SaveAsync(string key, CursorState state, string? etag, CancellationToken ct);
}

/// <summary>
/// A polled source's cursor record.
/// </summary>
/// <param name="Committed">The cursor up to which every item has been handled.</param>
/// <param name="Pending">The upper bound of a page whose items are not yet known to be handled, or
/// <see langword="null"/>. Set before the page's items are handled and cleared when the cursor is
/// committed, so a run that finds it set reads exactly <c>(Committed, Pending]</c> again.</param>
public sealed record CursorState(string? Committed, string? Pending);

/// <summary>
/// A cursor record as read, with the ETag a save must match.
/// </summary>
/// <param name="State">The record.</param>
/// <param name="ETag">Its version.</param>
public sealed record StoredCursor(CursorState State, string ETag);

/// <summary>
/// A cursor record changed between this run's read and its save: another run of the component is
/// reading the same source. The cursor is not moved.
/// </summary>
public sealed class CursorConflictException : Exception
{
    /// <summary>Creates the exception.</summary>
    public CursorConflictException()
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">What changed.</param>
    public CursorConflictException(string message) : base(message)
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">What changed.</param>
    /// <param name="inner">The cause.</param>
    public CursorConflictException(string message, Exception inner) : base(message, inner)
    {
    }
}

/// <summary>
/// A <see cref="ICursorStore"/> on a Dapr state store, such as a Postgres or SQL Server state-store
/// component backed by its own database. Saves are first-write-wins with strong consistency.
/// </summary>
/// <param name="dapr">The Dapr client.</param>
/// <param name="storeName">The state store component's name.</param>
public sealed class DaprCursorStore(DaprClient dapr, string storeName) : ICursorStore
{
    private static readonly StateOptions Options = new()
    {
        Concurrency = ConcurrencyMode.FirstWrite,
        Consistency = ConsistencyMode.Strong
    };

    /// <inheritdoc />
    public async Task<StoredCursor?> GetAsync(string key, CancellationToken ct)
    {
        var (state, etag) = await dapr.GetStateAndETagAsync<CursorState?>(storeName, key, ConsistencyMode.Strong,
            cancellationToken: ct);
        return state is null ? null : new StoredCursor(state, etag);
    }

    /// <inheritdoc />
    public async Task<string> SaveAsync(string key, CursorState state, string? etag, CancellationToken ct)
    {
        if (!await dapr.TrySaveStateAsync(storeName, key, state, etag ?? "", Options, cancellationToken: ct))
            throw new CursorConflictException(
                $"The cursor '{key}' in state store '{storeName}' changed since this run read it: another run is " +
                "reading the same source. The cursor was not moved.");

        // A Dapr save does not return the new ETag: read it back, and check that this run's save is
        // what is stored.
        var (stored, newEtag) = await dapr.GetStateAndETagAsync<CursorState?>(storeName, key, ConsistencyMode.Strong,
            cancellationToken: ct);
        if (stored != state)
            throw new CursorConflictException(
                $"The cursor '{key}' in state store '{storeName}' was changed by another run right after this run saved it.");
        return newEtag;
    }
}
