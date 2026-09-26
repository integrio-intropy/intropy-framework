using System.Text;
using Intropy.Framework.Adapters.Common;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Defines a contract for file system operations, providing methods to list, read, write, delete, and move files.
/// </summary>
/// <remarks>
/// Every method accepts a <see cref="CancellationToken"/>, flowing the caller's cancellation into
/// the underlying I/O where the transport supports it. Implementations may interleave their own
/// checks with transport-level cancellation.
/// </remarks>
public interface IFileAdapter
{
    /// <summary>
    /// Lists available files.
    /// </summary>
    /// <param name="ct">A cancellation token that can cancel the listing.</param>
    /// <returns>A list of <see cref="Common.FileEntry"/> that contains metadata about the files.</returns>
    Task<List<FileEntry>> ListAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets the content of a file.
    /// </summary>
    /// <param name="fileName">The name of the file.</param>
    /// <param name="ct">A cancellation token that can cancel the read.</param>
    /// <returns>A binary representation of the file content.</returns>
    /// <exception cref="Exception">Implementations throw on adapter or binding failure, including a
    /// missing file. The concrete exception type is adapter-specific: Dapr-binding adapters
    /// propagate the binding's exception.</exception>
    Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default);

    /// <summary>
    /// Gets the content of a file.
    /// </summary>
    /// <param name="fileName">The name of the file.</param>
    /// <param name="encoding">The encoding to use to decode the binary content.</param>
    /// <param name="ct">A cancellation token that can cancel the read.</param>
    /// <returns>A string representation of the file content. An empty file reads as
    /// <see cref="string.Empty"/> — never <see langword="null"/>.</returns>
    /// <exception cref="Exception">Implementations throw on adapter or binding failure, including a
    /// missing file. The concrete exception type is adapter-specific: Dapr-binding adapters
    /// propagate the binding's exception.</exception>
    Task<string> GetContentAsync(string fileName, Encoding encoding, CancellationToken ct = default);

    /// <summary>
    /// Creates a file and writes the binary content.
    /// </summary>
    /// <param name="fileName">The name of the file to create.</param>
    /// <param name="content">The binary representation of the file content to write.</param>
    /// <param name="basePathOverride">Overrides the base path provided in the configuration.</param>
    /// <param name="ct">A cancellation token that can cancel the write.</param>
    Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null, CancellationToken ct = default);

    /// <summary>
    /// Creates a file and writes the string content.
    /// </summary>
    /// <param name="fileName">The name of the file to create.</param>
    /// <param name="content">The string representation of the file content to write.</param>
    /// <param name="encoding">The encoding to use to convert the string to binary.</param>
    /// <param name="basePathOverride">Overrides the base path provided in the configuration.</param>
    /// <param name="ct">A cancellation token that can cancel the write.</param>
    Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null, CancellationToken ct = default);

    /// <summary>
    /// Deletes a file.
    /// </summary>
    /// <param name="fileName">The name of the file to delete.</param>
    /// <param name="ct">A cancellation token that can cancel the delete.</param>
    Task DeleteAsync(string fileName, CancellationToken ct = default);
}
