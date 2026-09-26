using System.Text.Json;
using Dapr.Client;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Adapter for local file storage that uses Dapr bindings.localstorage to perform basic file operations.
/// Useful when an SMB share or other file system is mounted to the pod.
/// </summary>
/// <param name="daprClient">The Dapr client the binding calls go through.</param>
/// <param name="options">The adapter's binding name, base path, and optional listing regex.</param>
public class LocalFileAdapter(DaprClient daprClient, FileAdapterOptions options)
    : DaprBindingFileAdapter(daprClient, options)
{
    /// <inheritdoc/>
    internal override string AdapterKind => "local";

    /// <summary>bindings.localstorage carries the file path under <c>fileName</c>.</summary>
    internal override string FileNameMetadataKey => "fileName";

    /// <summary>Joins with <c>/</c>; an empty base path leaves the file name unchanged.</summary>
    internal override string CombinePath(string basePath, string fileName) =>
        string.IsNullOrEmpty(basePath) ? fileName : $"{basePath}/{fileName}";

    internal override List<string> ParseListResponse(BindingResponse response)
    {
        var fullPaths = JsonSerializer.Deserialize<List<string>>(response.Data.Span) ?? [];

        // Extract just the filename from the full path (localstorage returns full paths like /storage/basePath/file.txt)
        return fullPaths
            .Select(path => path.Split('/').Last())
            .Where(fileName => !string.IsNullOrEmpty(fileName))
            .ToList();
    }
}
