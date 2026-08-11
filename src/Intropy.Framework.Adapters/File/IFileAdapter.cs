using System.Text;
using FileInfo = Intropy.Framework.Adapters.Common.FileInfo;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Defines a contract for file system operations, providing methods to list, read, write, delete, and move files.
/// </summary>
public interface IFileAdapter
{
    /// <summary>
    /// Lists available files.
    /// </summary>
    /// <returns>A list of <see cref="Common.FileInfo"/> that contains metadata about the files.</returns>
    Task<List<FileInfo>> ListAsync();

    /// <summary>
    /// Gets the content of a file.
    /// </summary>
    /// <param name="fileName">The name of the file.</param>
    /// <returns>A binary representation of the file content.</returns>
    /// <exception cref="Exception">Implementations throw on adapter or binding failure, including a
    /// missing file. The concrete exception type is adapter-specific: Dapr-binding adapters
    /// propagate the binding's exception.</exception>
    Task<byte[]> GetContentAsync(string fileName);

    /// <summary>
    /// Gets the content of a file.
    /// </summary>
    /// <param name="fileName">The name of the file.</param>
    /// <param name="encoding">The encoding to use to decode the binary content.</param>
    /// <returns>A string representation of the file content.</returns>
    /// <exception cref="Exception">Implementations throw on adapter or binding failure, including a
    /// missing file. The concrete exception type is adapter-specific: Dapr-binding adapters
    /// propagate the binding's exception.</exception>
    Task<string?> GetContentAsync(string fileName, Encoding encoding);

    /// <summary>
    /// Creates a file and writes the binary content.
    /// </summary>
    /// <param name="fileName">The name of the file to create.</param>
    /// <param name="content">The binary representation of the file content to write.</param>
    /// <param name="basePathOverride">Overrides the base path provided in the configuration.</param>
    Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null);

    /// <summary>
    /// Creates a file and writes the string content.
    /// </summary>
    /// <param name="fileName">The name of the file to create.</param>
    /// <param name="content">The string representation of the file content to write.</param>
    /// <param name="encoding">The encoding to use to convert the string to binary.</param>
    /// <param name="basePathOverride">Overrides the base path provided in the configuration.</param>
    Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null);

    /// <summary>
    /// Deletes a file.
    /// </summary>
    /// <param name="fileName">The name of the file to delete.</param>
    Task DeleteAsync(string fileName);
}
