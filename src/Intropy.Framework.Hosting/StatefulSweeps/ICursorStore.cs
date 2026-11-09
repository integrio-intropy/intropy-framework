using Dapr.Client;

namespace Intropy.Framework.Hosting.StatefulSweeps;

/// <summary>
/// Where <see cref="StatefulSweep"/> keeps a polled source's cursor: one record per component and
/// port.
/// </summary>
/// <remarks>
/// The default is a Dapr state store (<see cref="DaprCursorStore"/>), named by the source's
/// <see cref="CursorOptions.StoreName"/>. Register an implementation keyed by that store name to keep
/// cursors somewhere else.
/// </remarks>
public interface ICursorStore
{
    /// <summary>Reads a cursor record.</summary>
    /// <param name="key">The record's key.</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>The record, or <see langword="null"/> when none is saved.</returns>
    Task<CursorState?> GetAsync(string key, CancellationToken ct);

    /// <summary>Saves a cursor record, replacing the previous one.</summary>
    /// <param name="key">The record's key.</param>
    /// <param name="state">The record.</param>
    /// <param name="ct">A cancellation token.</param>
    Task SaveAsync(string key, CursorState state, CancellationToken ct);
}

/// <summary>
/// A polled source's cursor record.
/// </summary>
/// <param name="Committed">The cursor up to which every message has been handled.</param>
/// <param name="Pending">The upper bound of a read whose message is not yet known to be handled, or
/// <see langword="null"/>. Set before the message is handled and cleared when the cursor is
/// committed, so a run that finds it set reads exactly <c>(Committed, Pending]</c> again.</param>
public sealed record CursorState(string? Committed, string? Pending);

/// <summary>
/// A <see cref="ICursorStore"/> on a Dapr state store, such as a Postgres or SQL Server state-store
/// component backed by its own database.
/// </summary>
/// <param name="dapr">The Dapr client.</param>
/// <param name="storeName">The state store component's name.</param>
public sealed class DaprCursorStore(DaprClient dapr, string storeName) : ICursorStore
{
    /// <inheritdoc />
    public Task<CursorState?> GetAsync(string key, CancellationToken ct) =>
        dapr.GetStateAsync<CursorState?>(storeName, key, cancellationToken: ct);

    /// <inheritdoc />
    public Task SaveAsync(string key, CursorState state, CancellationToken ct) =>
        dapr.SaveStateAsync(storeName, key, state, cancellationToken: ct);
}
