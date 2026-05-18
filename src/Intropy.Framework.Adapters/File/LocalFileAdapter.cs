using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Dapr.Client;
using Intropy.Framework.Adapters.Common;
using FileInfo = Intropy.Framework.Adapters.Common.FileInfo;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Adapter for local file storage that uses Dapr bindings.localstorage to perform basic file operations.
/// Useful when an SMB share or other file system is mounted to the pod.
/// </summary>
/// <param name="daprClient"></param>
/// <param name="options"></param>
public class LocalFileAdapter(DaprClient daprClient, FileAdapterOptions options) : IFileAdapter
{
    private const string ListOperation = "list";
    private const string GetOperation = "get";
    private const string CreateOperation = "create";
    private const string DeleteOperation = "delete";

    /// <inheritdoc/>
    public async Task<List<FileInfo>> ListAsync()
    {
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        try
        {
            var request = new BindingRequest(options.DaprBindingName, ListOperation)
                { Metadata = { ["fileName"] = options.BasePath } };
            var response = await daprClient.InvokeBindingAsync(request);

            var fullPaths = JsonSerializer.Deserialize<List<string>>(response.Data.Span) ?? [];

            // Extract just the filename from the full path (localstorage returns full paths like /storage/basePath/file.txt)
            var files = fullPaths
                .Select(path => path.Split('/').Last())
                .Where(fileName => !string.IsNullOrEmpty(fileName))
                .ToList();

            if (options.FileNameRegex is not null)
                files = FilterFiles(files, options.FileNameRegex);

            return files
                .Select(fileName => new FileInfo(fileName))
                .ToList();
        }
        catch (Exception e)
        {
            activity?.AddException(e);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<byte[]> GetContentAsync(string fileName)
    {
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        var filePath = CombinePath(options.BasePath, fileName);
        var request = new BindingRequest(options.DaprBindingName, GetOperation)
            { Metadata = { ["fileName"] = filePath } };

        try
        {
            var response = await daprClient.InvokeBindingAsync(request);
            var data = response.Data.ToArray();
            return data;
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
        var content = encoding.GetString(data);
        return content;
    }

    /// <inheritdoc/>
    public async Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null)
    {
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        var basePath = basePathOverride ?? options.BasePath;
        var filePath = CombinePath(basePath, fileName);
        var request = new BindingRequest(options.DaprBindingName, CreateOperation)
        {
            Data = content,
            Metadata = { ["fileName"] = filePath }
        };

        try
        {
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
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        var filePath = CombinePath(options.BasePath, fileName);
        var request = new BindingRequest(options.DaprBindingName, DeleteOperation)
            { Metadata = { ["fileName"] = filePath } };

        try
        {
            await daprClient.InvokeBindingAsync(request);
        }
        catch (Exception ex)
        {
            activity?.AddException(ex);
            throw;
        }
    }

    private static List<string> FilterFiles(List<string> fileNames, Regex regex) =>
        fileNames.Where(fileName => regex.IsMatch(fileName)).ToList();

    private static string CombinePath(string basePath, string fileName) =>
        string.IsNullOrEmpty(basePath) ? fileName : $"{basePath}/{fileName}";
}
