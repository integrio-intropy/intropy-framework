using System.Text;
using Intropy.Framework.Adapters.File;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// One listed source file, handed to the file handler inside its own dependency-injection scope.
/// Content is read on demand, once: the bytes are kept for archiving.
/// </summary>
/// <remarks>
/// A polled source's read (<see cref="StatefulSweeps.StatefulSweep"/>) is handed over the same way, so a
/// component's pipelines take it like a file: its content is already read, it has no file name, and it
/// brings the context metadata its source set.
/// </remarks>
public sealed class SweptFile
{
    private readonly IFileAdapter? _source;
    private byte[]? _content;

    internal SweptFile(string name, string port, IFileAdapter source, IServiceProvider services)
    {
        Name = name;
        Port = port;
        _source = source;
        Services = services;
    }

    /// <summary>A polled source's read: content already in hand, and no file behind it.</summary>
    internal SweptFile(string name, string port, byte[] content, IReadOnlyDictionary<string, string> metadata,
        IServiceProvider services)
    {
        Name = name;
        Port = port;
        _content = content;
        Metadata = metadata;
        Services = services;
    }

    /// <summary>The file's name in the source. For a polled source's read, a description of the read
    /// (the port and its range), for logs; it is not a file name and is not set on the context.</summary>
    public string Name { get; }

    /// <summary>Whether this is a listed file; <see langword="false"/> for a polled source's read.</summary>
    internal bool IsFile => _source is not null;

    /// <summary>Entries the item's source set for its context metadata.</summary>
    internal IReadOnlyDictionary<string, string> Metadata { get; } = new Dictionary<string, string>();

    /// <summary>The source port the file was listed from.</summary>
    public string Port { get; }

    /// <summary>The file's own scope: resolve per-file services (such as the pipeline) here.</summary>
    public IServiceProvider Services { get; }

    /// <summary>Reads the file's content.</summary>
    /// <param name="ct">A cancellation token forwarded to the source adapter.</param>
    public async Task<byte[]> ReadAsync(CancellationToken ct = default) =>
        _content ??= await _source!.GetContentAsync(Name, ct);

    /// <summary>Reads the file's content as text, decoded with <paramref name="encoding"/> from
    /// the bytes <see cref="ReadAsync"/> fetched and kept: reading the text never fetches the file
    /// again, and an empty file reads as <see cref="string.Empty"/>.</summary>
    /// <param name="encoding">The encoding to use to decode the binary content.</param>
    /// <param name="ct">A cancellation token forwarded to the source adapter.</param>
    public async Task<string> ReadTextAsync(Encoding encoding, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(encoding);
        return encoding.GetString(await ReadAsync(ct));
    }
}
