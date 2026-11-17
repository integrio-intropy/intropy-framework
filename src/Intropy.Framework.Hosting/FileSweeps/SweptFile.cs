using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Sweeps;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// One listed source file, handed to the file handler inside its own dependency-injection scope.
/// Content is read on demand, once: the bytes are kept for archiving.
/// </summary>
public sealed class SweptFile : SweptItem
{
    private readonly IFileAdapter _source;
    private byte[]? _content;

    internal SweptFile(string name, string port, IFileAdapter source, IServiceProvider services)
        : base(name, port, services)
    {
        _source = source;
        Metadata = new Dictionary<string, string> { [SourceContextKeys.FileName] = name };
    }

    /// <summary>The file's name in the source; the item's <see cref="SweptItem.Id"/>.</summary>
    public string Name => Id;

    /// <summary>The file's name, as <see cref="SourceContextKeys.FileName"/>.</summary>
    public override IReadOnlyDictionary<string, string> Metadata { get; }

    /// <summary>Reads the file's content. The bytes are fetched once and kept: reading again,
    /// or as text, never fetches the file again, and an empty file reads as an empty array.</summary>
    /// <param name="ct">A cancellation token forwarded to the source adapter.</param>
    public override async Task<byte[]> ReadAsync(CancellationToken ct = default) =>
        _content ??= await _source.GetContentAsync(Name, ct);
}
