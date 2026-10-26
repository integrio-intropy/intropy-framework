using System.Globalization;
using System.Text.Json;
using Dapr.Client;
using Intropy.Framework.Adapters.Common;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Adapter for SFTP that uses Dapr to perform basic file operations.
/// </summary>
/// <param name="daprClient">The Dapr client the binding calls go through.</param>
/// <param name="options">The adapter's binding name, base path, and optional listing regex.</param>
public class SftpAdapter(DaprClient daprClient, FileAdapterOptions options)
    : DaprBindingFileAdapter(daprClient, options)
{
    /// <inheritdoc/>
    internal override string AdapterKind => "sftp";

    /// <summary>bindings.sftp carries the file path under <c>fileName</c>.</summary>
    internal override string FileNameMetadataKey => "fileName";

    /// <summary>Joins with <see cref="Path.Combine(string, string)"/>; absolute paths keep their leading slash.</summary>
    internal override string CombinePath(string basePath, string fileName) =>
        Path.Combine(basePath, fileName);

    internal override List<FileEntry> ParseListResponse(BindingResponse response) => ParseSftpListing(response);

    /// <summary>Parses the SFTP binding's listing, <c>[{"fileName", "isDirectory"}]</c>, into its
    /// files. The bindings that follow its contract (<see cref="SmbAdapter"/>) answer in the same
    /// shape, some with <c>size</c> and <c>modTime</c> (RFC 3339) as well.</summary>
    internal static List<FileEntry> ParseSftpListing(BindingResponse response)
    {
        var fileObjects = JsonSerializer.Deserialize<List<JsonElement>>(response.Data.Span) ?? [];

        return fileObjects
            .Where(obj => obj.ValueKind == JsonValueKind.Object &&
                          obj.TryGetProperty("isDirectory", out var isDir) &&
                          !isDir.GetBoolean() &&
                          obj.TryGetProperty("fileName", out _))
            .Select(obj => new FileEntry(
                obj.GetProperty("fileName").GetString() ?? "",
                obj.TryGetProperty("size", out var size) && size.TryGetInt64(out var bytes) ? bytes : null,
                obj.TryGetProperty("modTime", out var modTime) &&
                DateTimeOffset.TryParse(modTime.GetString(), CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal, out var modified)
                    ? modified
                    : null))
            .Where(file => !string.IsNullOrEmpty(file.Name))
            .ToList();
    }
}
