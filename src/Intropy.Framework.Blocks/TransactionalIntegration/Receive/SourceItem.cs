using System.Diagnostics.CodeAnalysis;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive;

/// <summary>
/// A source item with its content, ready for publishing.
/// </summary>
/// <param name="Id">Unique identifier for the item (e.g., file name).</param>
/// <param name="Data">The binary content of the item.</param>
[SuppressMessage("Performance", "CA1819:Properties should not return arrays",
    Justification = "Data carries raw file content; byte[] is the natural shape for file I/O APIs.")]
public record SourceItem(
    string Id,
    byte[] Data
)
{
    /// <summary>The source port the item was swept from, when the framework swept it. The send side
    /// of a Transactional Integration with send routes routes on it, and on <see cref="Id"/>.</summary>
    public string? Port { get; init; }
}
