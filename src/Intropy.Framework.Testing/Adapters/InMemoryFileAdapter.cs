using System.Text;
using Intropy.Framework.Adapters.File;
using FileInfo = Intropy.Framework.Adapters.Common.FileInfo;

namespace Intropy.Framework.Testing.Adapters;

/// <summary>
/// A stateful, in-memory <see cref="IFileAdapter"/> fake for component integration tests.
/// </summary>
/// <remarks>
/// <para>
/// Files are keyed on the <em>effective path</em>: <c>basePathOverride + "/" + fileName</c> when an
/// override is passed to a write (mirroring <c>LocalFileAdapter.CombinePath</c>), else just
/// <c>fileName</c>. Keys are compared ordinal-ignore-case.
/// </para>
/// <para>
/// Missing-file behavior matches production: reads throw <see cref="FileNotFoundException"/>
/// (the Dapr binding throws; it never returns null), while deletes no-op (binding delete is
/// idempotent). Unlike <c>LocalFileAdapter</c>, <see cref="ListAsync"/> returns every file in the
/// store — the fake has no configured base path to filter on.
/// </para>
/// <para>
/// Use <see cref="ReadException"/> and <see cref="WriteException"/> to simulate a dead source or
/// destination. All state is guarded by a lock; assertion members return snapshots.
/// </para>
/// </remarks>
public sealed class InMemoryFileAdapter : IFileAdapter
{
    private readonly Dictionary<string, Entry> _files = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _lock = new();

    /// <summary>
    /// Seeds a file with UTF-8 encoded string content.
    /// </summary>
    /// <param name="fileName">The effective-path key of the file.</param>
    /// <param name="content">The string content, encoded as UTF-8.</param>
    /// <returns>This adapter, for fluent seeding.</returns>
    public InMemoryFileAdapter AddFile(string fileName, string content)
    {
        ArgumentNullException.ThrowIfNull(content);
        return AddFile(fileName, Encoding.UTF8.GetBytes(content));
    }

    /// <summary>
    /// Seeds a file with binary content.
    /// </summary>
    /// <param name="fileName">The effective-path key of the file.</param>
    /// <param name="content">The binary content.</param>
    /// <returns>This adapter, for fluent seeding.</returns>
    public InMemoryFileAdapter AddFile(string fileName, byte[] content)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(content);

        lock (_lock)
        {
            _files[fileName] = new Entry(content);
        }

        return this;
    }

    /// <summary>
    /// Seeds a file that is listed but cannot be read: both <c>GetContentAsync</c> overloads throw
    /// <paramref name="exception"/> for this file while other files read fine. Simulates a
    /// corrupt or inaccessible source file.
    /// </summary>
    /// <param name="fileName">The effective-path key of the file.</param>
    /// <param name="exception">The exception thrown when the file is read. Defaults to an
    /// <see cref="InvalidOperationException"/>.</param>
    /// <returns>This adapter, for fluent seeding.</returns>
    public InMemoryFileAdapter AddUnreadableFile(string fileName, Exception? exception = null)
    {
        ArgumentNullException.ThrowIfNull(fileName);

        lock (_lock)
        {
            _files[fileName] = new Entry(readException: exception ?? new InvalidOperationException(
                $"File cannot be read: {fileName}"));
        }

        return this;
    }

    /// <summary>
    /// Makes <see cref="DeleteAsync"/> throw for a specific file while other deletes succeed.
    /// Simulates the publish-succeeds-but-delete-fails path, where the file is re-processed on the
    /// next run and idempotency must catch it. Set <paramref name="exception"/> to null to restore
    /// normal behavior for the file.
    /// </summary>
    /// <param name="fileName">The effective-path key of the file.</param>
    /// <param name="exception">The exception thrown when the file is deleted, or null to restore
    /// normal behavior.</param>
    /// <returns>This adapter, for fluent setup.</returns>
    public InMemoryFileAdapter SetDeleteException(string fileName, Exception? exception)
    {
        ArgumentNullException.ThrowIfNull(fileName);

        lock (_lock)
        {
            if (_files.TryGetValue(fileName, out var entry))
            {
                entry.DeleteException = exception;
            }
            else if (exception is not null)
            {
                _files[fileName] = new Entry([]) { DeleteException = exception };
            }
        }

        return this;
    }

    /// <summary>
    /// Gets a snapshot of all files, keyed by effective path.
    /// </summary>
    public IReadOnlyDictionary<string, byte[]> Files
    {
        get
        {
            lock (_lock)
            {
                return new Dictionary<string, byte[]>(
                    _files
                        .Where(kv => kv.Value.Content is not null)
                        .Select(kv => new KeyValuePair<string, byte[]>(kv.Key, [.. kv.Value.Content!])),
                    StringComparer.OrdinalIgnoreCase);
            }
        }
    }

    /// <summary>
    /// When set, thrown by <see cref="ListAsync"/> and both <c>GetContentAsync</c> overloads to
    /// simulate a dead source. Clearing the property restores normal behavior.
    /// </summary>
    public Exception? ReadException { get; set; }

    /// <summary>
    /// When set, thrown by both <c>WriteAsync</c> overloads to simulate a dead destination.
    /// Clearing the property restores normal behavior.
    /// </summary>
    public Exception? WriteException { get; set; }

    /// <summary>
    /// When set, thrown by <see cref="DeleteAsync"/> for every file to simulate a source that
    /// refuses deletes — the publish-succeeds-but-delete-fails path, where the file is
    /// re-processed on the next run and idempotency must catch it. For per-file delete failures,
    /// use <see cref="SetDeleteException"/>. Clearing the property restores normal behavior.
    /// </summary>
    public Exception? DeleteException { get; set; }

    /// <summary>
    /// Gets a seeded or written file's content as a UTF-8 string.
    /// </summary>
    /// <param name="fileName">The plain file name.</param>
    /// <returns>The UTF-8 decoded content.</returns>
    /// <exception cref="FileNotFoundException">Thrown when no file exists under the key.</exception>
    public string GetString(string fileName) =>
        Encoding.UTF8.GetString(Read(fileName));

    /// <summary>
    /// Gets an override-written file's content as a UTF-8 string.
    /// </summary>
    /// <param name="basePath">The base path override the file was written with.</param>
    /// <param name="fileName">The file name.</param>
    /// <returns>The UTF-8 decoded content.</returns>
    /// <exception cref="FileNotFoundException">Thrown when no file exists under the effective path.</exception>
    public string GetString(string basePath, string fileName) =>
        Encoding.UTF8.GetString(Read(CombinePath(basePath, fileName)));

    /// <inheritdoc/>
    /// <remarks>Returns every file in the store; the fake has no configured base path to filter on.</remarks>
    public Task<List<FileInfo>> ListAsync()
    {
        ThrowIfSet(ReadException);

        lock (_lock)
        {
            return Task.FromResult(_files.Keys.Select(key => new FileInfo(key)).ToList());
        }
    }

    /// <inheritdoc/>
    /// <exception cref="FileNotFoundException">Thrown when the file does not exist, matching the Dapr binding.</exception>
    public Task<byte[]> GetContentAsync(string fileName)
    {
        ThrowIfSet(ReadException);
        return Task.FromResult(Read(fileName));
    }

    /// <inheritdoc/>
    /// <exception cref="FileNotFoundException">Thrown when the file does not exist, matching the Dapr binding.</exception>
    public async Task<string?> GetContentAsync(string fileName, Encoding encoding)
    {
        ArgumentNullException.ThrowIfNull(encoding);
        var data = await GetContentAsync(fileName);
        return encoding.GetString(data);
    }

    /// <inheritdoc/>
    public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null)
    {
        ArgumentNullException.ThrowIfNull(content);
        ThrowIfSet(WriteException);

        lock (_lock)
        {
            _files[CombinePath(basePathOverride, fileName)] = new Entry(content);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null)
    {
        ArgumentNullException.ThrowIfNull(content);
        ArgumentNullException.ThrowIfNull(encoding);
        return WriteAsync(fileName, encoding.GetBytes(content), basePathOverride);
    }

    /// <inheritdoc/>
    /// <remarks>Deleting a missing file no-ops, matching the idempotent Dapr binding delete. Throws
    /// <see cref="DeleteException"/> when set, or a per-file exception configured via
    /// <see cref="SetDeleteException"/>; the file is left in the store, as in production.</remarks>
    public Task DeleteAsync(string fileName)
    {
        ThrowIfSet(DeleteException);

        lock (_lock)
        {
            if (_files.TryGetValue(fileName, out var entry))
            {
                ThrowIfSet(entry.DeleteException);
                _files.Remove(fileName);
            }
        }

        return Task.CompletedTask;
    }

    private byte[] Read(string key)
    {
        lock (_lock)
        {
            if (_files.TryGetValue(key, out var entry))
            {
                ThrowIfSet(entry.ReadException);
                return [.. entry.Content!];
            }
        }

        throw new FileNotFoundException($"File not found: {key}", key);
    }

    private static void ThrowIfSet(Exception? exception)
    {
        if (exception is not null)
        {
            throw exception;
        }
    }

    private static string CombinePath(string? basePath, string fileName) =>
        string.IsNullOrEmpty(basePath) ? fileName : $"{basePath}/{fileName}";

    private sealed class Entry(byte[]? content = null, Exception? readException = null)
    {
        public byte[]? Content { get; } = content is null ? null : [.. content];
        public Exception? ReadException { get; } = readException;
        public Exception? DeleteException { get; set; }
    }
}
