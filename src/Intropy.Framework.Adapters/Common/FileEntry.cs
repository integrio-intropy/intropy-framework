namespace Intropy.Framework.Adapters.Common;

/// <summary>
/// Metadata about a file
/// </summary>
/// <param name="Name">The name of the file</param>
/// <param name="Size">The file's size in bytes, when the binding's listing reports it (SMB does).</param>
/// <param name="ModifiedAt">When the file was last modified, when the binding's listing reports it
/// (SMB does).</param>
public record FileEntry(string Name, long? Size = null, DateTimeOffset? ModifiedAt = null);
