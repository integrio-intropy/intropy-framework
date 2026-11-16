using System.Globalization;
using Intropy.Framework.Hosting.StatefulSweeps;

namespace Intropy.Framework.Testing.StatefulSweeps;

/// <summary>
/// An <see cref="ICursorStore"/> fake for component tests of a polled source port, with the Dapr
/// store's first-write-wins semantics: every save must carry the ETag the record was read or last
/// saved with.
/// </summary>
/// <remarks>
/// <para>
/// Register it keyed by the source's store name (<see cref="CursorOptions.StoreName"/>), so the
/// sweep uses it instead of a Dapr state store:
/// <c>services.AddKeyedSingleton&lt;ICursorStore&gt;("statestore", store)</c>.
/// </para>
/// <para>
/// <see cref="Set"/> writes a record as someone else would (seeding a cursor, or another run moving
/// it): it changes the record's ETag, so a run that read the record before fails its next save with
/// <see cref="CursorConflictException"/>. Set <see cref="SaveFailure"/> to make saves fail, as an
/// unreachable store does. All state is guarded by a lock; assertion members return snapshots.
/// </para>
/// </remarks>
public sealed class InMemoryCursorStore : ICursorStore
{
    private readonly Dictionary<string, (CursorState State, long Version)> _records = new(StringComparer.Ordinal);
    private readonly List<CursorState> _saves = [];
    private readonly object _lock = new();
    private long _version;

    /// <summary>The saved records by key.</summary>
    public IReadOnlyDictionary<string, CursorState> State
    {
        get
        {
            lock (_lock)
                return _records.ToDictionary(r => r.Key, r => r.Value.State, StringComparer.Ordinal);
        }
    }

    /// <summary>Every record the sweep saved, in order (not those written with <see cref="Set"/>).</summary>
    public IReadOnlyList<CursorState> Saves
    {
        get
        {
            lock (_lock)
                return _saves.ToList();
        }
    }

    /// <summary>When set, decides whether a save fails: return an exception to throw it (the record is
    /// not saved), or <see langword="null"/> to save.</summary>
    public Func<CursorState, Exception?>? SaveFailure { get; set; }

    /// <summary>Writes a record as someone other than the sweep would, changing its ETag.</summary>
    /// <param name="key">The record's key (<c>{component}:{port}</c>).</param>
    /// <param name="state">The record.</param>
    public void Set(string key, CursorState state)
    {
        lock (_lock)
            _records[key] = (state, ++_version);
    }

    /// <inheritdoc />
    public Task<StoredCursor?> GetAsync(string key, CancellationToken ct)
    {
        lock (_lock)
            return Task.FromResult(_records.TryGetValue(key, out var record)
                ? new StoredCursor(record.State, ETagOf(record.Version))
                : null);
    }

    /// <inheritdoc />
    public Task<string> SaveAsync(string key, CursorState state, string? etag, CancellationToken ct)
    {
        if (SaveFailure?.Invoke(state) is { } failure)
            throw failure;

        lock (_lock)
        {
            if (_records.TryGetValue(key, out var record) && etag is not null && etag != ETagOf(record.Version))
                throw new CursorConflictException($"The cursor '{key}' changed since it was read.");
            if (etag is not null && !_records.ContainsKey(key))
                throw new CursorConflictException($"The cursor '{key}' was deleted since it was read.");
            if (etag is null && _records.ContainsKey(key))
                throw new CursorConflictException($"The cursor '{key}' was created since this run found none.");

            _records[key] = (state, ++_version);
            _saves.Add(state);
            return Task.FromResult(ETagOf(_version));
        }
    }

    private static string ETagOf(long version) => version.ToString(CultureInfo.InvariantCulture);
}
