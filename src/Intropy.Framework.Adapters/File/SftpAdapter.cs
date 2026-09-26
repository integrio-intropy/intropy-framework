using System.Text.Json;
using Dapr.Client;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Adapter for SFTP that uses Dapr to perform basic file operations.
/// </summary>
/// <param name="daprClient">The Dapr client the binding calls go through.</param>
/// <param name="options">The adapter's binding name, base path, and optional listing regex.</param>
public class SftpAdapter(DaprClient daprClient, FileAdapterOptions options)
    : DaprBindingFileAdapter(daprClient, options)
{
    /// <summary>bindings.sftp carries the file path under <c>fileName</c>.</summary>
    internal override string FileNameMetadataKey => "fileName";

    /// <summary>Joins with <see cref="Path.Combine(string, string)"/>; absolute paths keep their leading slash.</summary>
    internal override string CombinePath(string basePath, string fileName) =>
        Path.Combine(basePath, fileName);

    internal override List<string> ParseListResponse(BindingResponse response)
    {
        var fileObjects = JsonSerializer.Deserialize<List<JsonElement>>(response.Data.Span) ?? [];

        return fileObjects
            .Where(obj => obj.ValueKind == JsonValueKind.Object &&
                          obj.TryGetProperty("isDirectory", out var isDir) &&
                          !isDir.GetBoolean() &&
                          obj.TryGetProperty("fileName", out _))
            .Select(obj => obj.GetProperty("fileName").GetString() ?? "")
            .Where(fileName => !string.IsNullOrEmpty(fileName))
            .ToList();
    }
}
