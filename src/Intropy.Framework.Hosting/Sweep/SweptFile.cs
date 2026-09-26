using System.Text;
using Intropy.Framework.Adapters.File;

namespace Intropy.Framework.Hosting.Sweep;

/// <summary>
/// One listed source file, handed to the file handler inside its own dependency-injection scope.
/// Content is read on demand, once: the bytes are kept for archiving.
/// </summary>
public sealed class SweptFile
{
    private readonly IFileAdapter _source;
    private byte[]? _content;

    internal SweptFile(string name, IFileAdapter source, IServiceProvider services)
    {
        Name = name;
        _source = source;
        Services = services;
    }

    /// <summary>The file's name in the source.</summary>
    public string Name { get; }

    /// <summary>The file's own scope: resolve per-file services (such as the pipeline) here.</summary>
    public IServiceProvider Services { get; }

    /// <summary>Reads the file's content.</summary>
    /// <param name="ct">A cancellation token forwarded to the source adapter.</param>
    public async Task<byte[]> ReadAsync(CancellationToken ct = default) =>
        _content ??= await _source.GetContentAsync(Name, ct);

    /// <summary>Reads the file's content as text, with the adapter's decoding rules (an empty
    /// file reads as <see cref="string.Empty"/>).</summary>
    /// <param name="encoding">The encoding to use to decode the binary content.</param>
    /// <param name="ct">A cancellation token forwarded to the source adapter.</param>
    public Task<string> ReadTextAsync(Encoding encoding, CancellationToken ct = default) =>
        _source.GetContentAsync(Name, encoding, ct);
}
