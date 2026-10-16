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
    /// of a Transactional Integration with send routes routes on it.</summary>
    public string? Port { get; init; }

    /// <summary>The id the item's message will carry, minted before the claim check so the claim key
    /// can name it. Set by the framework, never by a step. <see langword="null"/> until something
    /// needs the id in advance, in which case the envelope mints its own.</summary>
    public string? MessageId { get; init; }

    /// <summary>The path the item's payload was written to in the claim store, when it was too large
    /// to ride inside the envelope. Set by the claim check; when it is set the envelope carries a
    /// <c>dataref</c> pointer and no data at all.</summary>
    public string? DataRef { get; init; }
}
