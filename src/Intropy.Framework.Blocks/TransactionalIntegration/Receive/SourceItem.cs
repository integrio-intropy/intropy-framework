using System.Diagnostics.CodeAnalysis;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive;

/// <summary>
/// Metadata about an item to be processed (returned by listing operation).
/// </summary>
/// <param name="Id">Unique identifier for the item (e.g., file name).</param>
public record SourceItemInfo(
    string Id
);

/// <summary>
/// An item with its content, ready for publishing.
/// </summary>
/// <param name="Id">Unique identifier for the item (e.g., file name).</param>
/// <param name="Data">The binary content of the item.</param>
[SuppressMessage("Performance", "CA1819:Properties should not return arrays",
    Justification = "Data carries raw file content; byte[] is the natural shape for file I/O APIs.")]
public record SourceItem(
    string Id,
    byte[] Data
);
