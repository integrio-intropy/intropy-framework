using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using Dapr.Client;
using Intropy.Framework.Adapters.Common;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// The shared Dapr-binding implementation behind <see cref="LocalFileAdapter"/>,
/// <see cref="SftpAdapter"/>, and <see cref="AzureBlobStorageAdapter"/>: the binding operation
/// constants, the activity/exception wrapper around each Dapr call, the encoding-based
/// <c>GetContentAsync</c>/<c>WriteAsync</c> overloads, regex filtering, and the shared
/// request/response flow. Each adapter supplies only what genuinely varies: the file-name
/// metadata key, path combination, list-request building/list-response parsing, and any extra
/// per-operation metadata.
/// </summary>
/// <remarks>
/// <para>
/// The class is deliberately a dead end for consumers: it has an internal constructor and the
/// members the framework adapters override are internal, so a custom adapter cannot derive from
/// it. Custom adapters implement <see cref="IFileAdapter"/> directly (or plug in through the
/// <c>AddFileAdapter</c> factory overload).
/// </para>
/// <para>An empty file's content decodes as <see cref="string.Empty"/> — never null.</para>
/// </remarks>
public abstract class DaprBindingFileAdapter : IFileAdapter
{
    internal const string ListOperation = "list";
    internal const string GetOperation = "get";
    internal const string CreateOperation = "create";
    internal const string DeleteOperation = "delete";

    private readonly FileAdapterOptions _options;

    internal DaprBindingFileAdapter(DaprClient daprClient, FileAdapterOptions options)
    {
        DaprClient = daprClient ?? throw new ArgumentNullException(nameof(daprClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>The Dapr client every binding call goes through.</summary>
    internal DaprClient DaprClient { get; }

    /// <summary>The adapter's configured binding name, base path, and optional listing regex.</summary>
    internal FileAdapterOptions Options => _options;

    /// <summary>The metadata key carrying the file path in get/create/delete requests, and the
    /// base path in <see cref="LocalFileAdapter"/>/<see cref="SftpAdapter"/> list requests.</summary>
    internal abstract string FileNameMetadataKey { get; }

    /// <summary>Joins a base path and a file name with the adapter's own semantics
    /// (the repository's Unix-style path, <see cref="Path.Combine(string, string)"/>, or a
    /// normalized blob name).</summary>
    internal abstract string CombinePath(string basePath, string fileName);

    /// <summary>Builds the list request. The default puts the configured base path under
    /// <see cref="FileNameMetadataKey"/>; adapters whose binding takes the prefix in the request
    /// body override this.</summary>
    internal virtual BindingRequest BuildListRequest() =>
        new(_options.DaprBindingName, ListOperation)
            { Metadata = { [FileNameMetadataKey] = _options.BasePath } };

    /// <summary>Parses a list response into plain file names (no regex filtering yet).</summary>
    internal abstract List<string> ParseListResponse(BindingResponse response);

    /// <summary>Adds adapter-specific metadata to a delete request, beyond the file path.</summary>
    internal virtual void AddDeleteMetadata(Dictionary<string, string> metadata)
    {
    }

    /// <inheritdoc/>
    public async Task<List<FileEntry>> ListAsync(CancellationToken ct = default)
    {
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        try
        {
            var response = await DaprClient.InvokeBindingAsync(BuildListRequest(), ct);

            var fileNames = ParseListResponse(response);

            if (_options.FileNameRegex is not null)
                fileNames = FilterFiles(fileNames, _options.FileNameRegex);

            return fileNames
                .Select(fileName => new FileEntry(fileName))
                .ToList();
        }
        catch (Exception e)
        {
            RecordFailure(activity, e, ct);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(fileName);

        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        var request = BuildPathRequest(GetOperation, _options.BasePath, fileName);

        try
        {
            var response = await DaprClient.InvokeBindingAsync(request, ct);
            return response.Data.ToArray();
        }
        catch (Exception e)
        {
            RecordFailure(activity, e, ct);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<string> GetContentAsync(string fileName, Encoding encoding, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(encoding);

        var data = await GetContentAsync(fileName, ct);
        var content = encoding.GetString(data);
        return content;
    }

    /// <inheritdoc/>
    public async Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(content);

        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        var basePath = basePathOverride ?? _options.BasePath;
        var request = BuildPathRequest(CreateOperation, basePath, fileName);
        request.Data = content;

        try
        {
            await DaprClient.InvokeBindingAsync(request, ct);
        }
        catch (Exception e)
        {
            RecordFailure(activity, e, ct);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task WriteAsync(string fileName, string content, Encoding encoding,
        string? basePathOverride = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(encoding);
        ArgumentNullException.ThrowIfNull(content);

        var data = encoding.GetBytes(content);
        await WriteAsync(fileName, data, basePathOverride, ct);
    }

    /// <inheritdoc/>
    public async Task DeleteAsync(string fileName, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(fileName);

        using var activity = ActivitySourceProvider.ActivitySource.StartActivity();

        var request = BuildPathRequest(DeleteOperation, _options.BasePath, fileName);
        AddDeleteMetadata(request.Metadata);

        try
        {
            await DaprClient.InvokeBindingAsync(request, ct);
        }
        catch (Exception e)
        {
            RecordFailure(activity, e, ct);
            throw;
        }
    }

    private BindingRequest BuildPathRequest(string operation, string basePath, string fileName) =>
        new(_options.DaprBindingName, operation)
            { Metadata = { [FileNameMetadataKey] = CombinePath(basePath, fileName) } };

    /// <summary>Marks the operation's span as failed. A cancellation the caller requested is a
    /// clean stop, not a failure of the operation, and leaves the span as it is.</summary>
    private static void RecordFailure(Activity? activity, Exception e, CancellationToken ct)
    {
        if (activity is null || (e is OperationCanceledException && ct.IsCancellationRequested))
            return;

        activity.AddException(e);
        activity.SetStatus(ActivityStatusCode.Error, e.Message);
    }

    private static List<string> FilterFiles(List<string> fileNames, Regex regex) =>
        fileNames.Where(fileName => regex.IsMatch(fileName)).ToList();
}
