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
    private readonly Dictionary<string, byte[]> _files = new(StringComparer.OrdinalIgnoreCase);
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
            _files[fileName] = [.. content];
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
                    _files.Select(kv => new KeyValuePair<string, byte[]>(kv.Key, [.. kv.Value])),
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
            _files[CombinePath(basePathOverride, fileName)] = [.. content];
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
    /// <remarks>Deleting a missing file no-ops, matching the idempotent Dapr binding delete.</remarks>
    public Task DeleteAsync(string fileName)
    {
        lock (_lock)
        {
            _files.Remove(fileName);
        }

        return Task.CompletedTask;
    }

    private byte[] Read(string key)
    {
        lock (_lock)
        {
            if (_files.TryGetValue(key, out var content))
            {
                return [.. content];
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
}
