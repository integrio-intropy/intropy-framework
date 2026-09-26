using System.Text.Json;
using Dapr.Client;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Adapter for Azure Blob Storage that uses Dapr to perform basic file operations.
/// </summary>
/// <param name="daprClient">The Dapr client the binding calls go through.</param>
/// <param name="options">The adapter's binding name, base path, and optional listing regex.</param>
public class AzureBlobStorageAdapter(DaprClient daprClient, FileAdapterOptions options)
    : DaprBindingFileAdapter(daprClient, options)
{
    private const string DeleteSnapshotsKey = "deleteSnapshots";

    /// <inheritdoc/>
    internal override string AdapterKind => "azure_blob";

    /// <summary>bindings.azure.blobstorage carries the blob path under <c>blobName</c>.</summary>
    internal override string FileNameMetadataKey => "blobName";

    /// <summary>Joins the trimmed base path and file name with <c>/</c>, dropping slash edges:
    /// base paths and file names are normalized so <c>/inbound/</c> + <c>/order.json</c> is
    /// <c>inbound/order.json</c>.</summary>
    internal override string CombinePath(string basePath, string fileName) => BuildBlobName(basePath, fileName);

    /// <summary>The blob binding takes the listing prefix in the request body, not metadata.</summary>
    internal override BindingRequest BuildListRequest()
    {
        var prefix = string.IsNullOrWhiteSpace(Options.BasePath)
            ? null
            : NormalizePrefix(Options.BasePath) + "/";

        return new BindingRequest(Options.DaprBindingName, ListOperation)
        {
            Data = prefix is null
                ? null
                : JsonSerializer.SerializeToUtf8Bytes(new { prefix })
        };
    }

    internal override List<string> ParseListResponse(BindingResponse response)
    {
        var root = JsonSerializer.Deserialize<JsonElement>(response.Data.Span);

        if (root.ValueKind != JsonValueKind.Array)
            return [];

        var fileNames = new List<string>();

        foreach (var item in root.EnumerateArray())
        {
            var name = ExtractBlobName(item);
            if (!string.IsNullOrWhiteSpace(name))
                fileNames.Add(name);
        }

        return fileNames;
    }

    internal override void AddDeleteMetadata(Dictionary<string, string> metadata)
    {
        // Safe default: delete base blob + any snapshots if they exist
        metadata[DeleteSnapshotsKey] = "include";
    }

    private static string? ExtractBlobName(JsonElement item)
    {
        if (item.ValueKind == JsonValueKind.String)
            return item.GetString();

        if (item.ValueKind == JsonValueKind.Object)
        {
            if (item.TryGetProperty("Name", out var nameProp) && nameProp.ValueKind == JsonValueKind.String)
                return nameProp.GetString();

            if (item.TryGetProperty("name", out var nameProp2) && nameProp2.ValueKind == JsonValueKind.String)
                return nameProp2.GetString();
        }

        return null;
    }

    private static string NormalizePrefix(string value) =>
        value.Trim().Trim('/');

    private static string BuildBlobName(string? basePath, string fileName)
    {
        var cleanFile = fileName.TrimStart('/');

        if (string.IsNullOrWhiteSpace(basePath))
            return cleanFile;

        var cleanBase = NormalizePrefix(basePath);
        return $"{cleanBase}/{cleanFile}";
    }
}
