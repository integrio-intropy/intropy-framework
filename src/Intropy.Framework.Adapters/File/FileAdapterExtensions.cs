using System.Text;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Encoding-based conveniences over <see cref="IFileAdapter"/>'s binary core. Decoding and
/// encoding live here once, so adapters implement the binary operations only.
/// </summary>
public static class FileAdapterExtensions
{
    /// <summary>
    /// Gets the content of a file, decoded with <paramref name="encoding"/>.
    /// </summary>
    /// <param name="adapter">The file adapter.</param>
    /// <param name="fileName">The name of the file.</param>
    /// <param name="encoding">The encoding to use to decode the binary content.</param>
    /// <param name="ct">A cancellation token that can cancel the read.</param>
    /// <returns>A string representation of the file content. An empty file reads as
    /// <see cref="string.Empty"/> — never <see langword="null"/>.</returns>
    /// <exception cref="Exception">Adapters throw on adapter or binding failure, including a
    /// missing file. The concrete exception type is adapter-specific: Dapr-binding adapters
    /// propagate the binding's exception.</exception>
    public static async Task<string> GetContentAsync(this IFileAdapter adapter, string fileName, Encoding encoding,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(adapter);
        ArgumentNullException.ThrowIfNull(encoding);

        var data = await adapter.GetContentAsync(fileName, ct);
        return encoding.GetString(data);
    }

    /// <summary>
    /// Creates a file and writes the string content.
    /// </summary>
    /// <param name="adapter">The file adapter.</param>
    /// <param name="fileName">The name of the file to create.</param>
    /// <param name="content">The string representation of the file content to write.</param>
    /// <param name="encoding">The encoding to use to convert the string to binary.</param>
    /// <param name="basePathOverride">Overrides the base path provided in the configuration.</param>
    /// <param name="ct">A cancellation token that can cancel the write.</param>
    public static Task WriteAsync(this IFileAdapter adapter, string fileName, string content, Encoding encoding,
        string? basePathOverride = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(adapter);
        ArgumentNullException.ThrowIfNull(content);
        ArgumentNullException.ThrowIfNull(encoding);

        return adapter.WriteAsync(fileName, encoding.GetBytes(content), basePathOverride, ct);
    }
}
