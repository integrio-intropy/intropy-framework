using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Dapr.Client;
using Intropy.Framework.Adapters.Common;
using FileInfo = Intropy.Framework.Adapters.Common.FileInfo;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Adapter for Azure Blob Storage that uses Dapr to perform basic file operations.
/// </summary>
public class AzureBlobStorageAdapter(DaprClient daprClient, FileAdapterOptions options) : IFileAdapter
{
    private const string ListOperation = "list";
    private const string GetOperation = "get";
    private const string CreateOperation = "create";
    private const string DeleteOperation = "delete";
    private const string DeleteSnapshotsKey = "deleteSnapshots";
    private const string BlobNameKey = "blobName";

    /// <inheritdoc/>
    public async Task<List<FileInfo>> ListAsync()
    {
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        try
        {
            var prefix = string.IsNullOrWhiteSpace(options.BasePath)
                ? null
                : NormalizePrefix(options.BasePath) + "/";

            var request = new BindingRequest(options.DaprBindingName, ListOperation)
            {
                Data = prefix is null
                    ? null
                    : JsonSerializer.SerializeToUtf8Bytes(new { prefix })
            };

            var response = await daprClient.InvokeBindingAsync(request);

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
            
            if (options.FileNameRegex is not null)
                fileNames = FilterFiles(fileNames, options.FileNameRegex);

            return fileNames.Select(n => new FileInfo(n)).ToList();
        }
        catch (Exception e)
        {
            activity?.AddException(e);
            throw;
        }
    }
    
    
    private static List<string> FilterFiles(List<string> fileNames, Regex regex) =>
        fileNames.Where(fileName => regex.IsMatch(fileName)).ToList();



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


    /// <inheritdoc/>
    public async Task<byte[]> GetContentAsync(string fileName)
    {
        ArgumentNullException.ThrowIfNull(fileName);

        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        try
        {
            var blobName = BuildBlobName(options.BasePath, fileName);

            var request = new BindingRequest(options.DaprBindingName, GetOperation)
            {
                Metadata = { [BlobNameKey] = blobName }
            };

            var response = await daprClient.InvokeBindingAsync(request);
            return response.Data.ToArray();
        }
        catch (Exception e)
        {
            Activity.Current?.AddException(e);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<string?> GetContentAsync(string fileName, Encoding encoding)
    {
        ArgumentNullException.ThrowIfNull(encoding);

        var data = await GetContentAsync(fileName);
        return data.Length == 0 ? null : encoding.GetString(data);
    }

    /// <inheritdoc/>
    public async Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(content);

        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        try
        {
            var basePath = basePathOverride ?? options.BasePath;
            var blobName = BuildBlobName(basePath, fileName);

            var request = new BindingRequest(options.DaprBindingName, CreateOperation)
            {
                Data = content,
                Metadata = { [BlobNameKey] = blobName }
            };

            await daprClient.InvokeBindingAsync(request);
        }
        catch (Exception ex)
        {
            activity?.AddException(ex);
            throw;
        }
    }
    
    /// <inheritdoc/>
    public async Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null)
    {
        ArgumentNullException.ThrowIfNull(encoding);
        ArgumentNullException.ThrowIfNull(content);

        var data = encoding.GetBytes(content);
        await WriteAsync(fileName, data, basePathOverride);
    }
    
    /// <inheritdoc/>
    public async Task DeleteAsync(string fileName)
    {
        ArgumentNullException.ThrowIfNull(fileName);

        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        try
        {
            var blobName = BuildBlobName(options.BasePath, fileName);

            var request = new BindingRequest(options.DaprBindingName, DeleteOperation)
            {
                Metadata =
                {
                    [BlobNameKey] = blobName,

                    // Safe default: delete base blob + any snapshots if they exist
                    [DeleteSnapshotsKey] = "include"
                }
            };

            await daprClient.InvokeBindingAsync(request);
        }
        catch (Exception ex)
        {
            activity?.AddException(ex);
            throw;
        }
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
