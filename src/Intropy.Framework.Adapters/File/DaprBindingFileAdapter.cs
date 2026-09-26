using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using Dapr.Client;
using Intropy.Framework.Adapters.Common;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// The shared Dapr-binding implementation behind <see cref="LocalFileAdapter"/>,
/// <see cref="SftpAdapter"/>, and <see cref="AzureBlobStorageAdapter"/>: the binding operation
/// constants, the span around each Dapr call (named <c>{operation} {binding}</c>, with the
/// binding, path and size as attributes), the encoding-based
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

    /// <summary>The adapter's kind on its spans (<c>intropy.file.adapter</c>): <c>local</c>,
    /// <c>sftp</c> or <c>azure_blob</c>.</summary>
    internal abstract string AdapterKind { get; }

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
    public Task<List<FileEntry>> ListAsync(CancellationToken ct = default) =>
        TraceAsync(ListOperation, async activity =>
        {
            activity?.SetTag("file.directory", _options.BasePath);
            var response = await DaprClient.InvokeBindingAsync(BuildListRequest(), ct);

            var fileNames = ParseListResponse(response);
            activity?.SetTag("intropy.file.listed", fileNames.Count);

            if (_options.FileNameRegex is not null)
                fileNames = FilterFiles(fileNames, _options.FileNameRegex);
            // Listed but filtered out is the usual answer to "why wasn't my file picked up?".
            activity?.SetTag("intropy.file.matched", fileNames.Count);

            return fileNames
                .Select(fileName => new FileEntry(fileName))
                .ToList();
        }, ct);

    /// <inheritdoc/>
    public async Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(fileName);

        return await TraceAsync(GetOperation, async activity =>
        {
            var request = BuildPathRequest(GetOperation, _options.BasePath, fileName, activity);
            var response = await DaprClient.InvokeBindingAsync(request, ct);
            var content = response.Data.ToArray();
            activity?.SetTag("file.size", content.Length);
            return content;
        }, ct);
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

        await TraceAsync(CreateOperation, async activity =>
        {
            var request = BuildPathRequest(CreateOperation, basePathOverride ?? _options.BasePath, fileName, activity);
            request.Data = content;
            activity?.SetTag("file.size", content.Length);
            await DaprClient.InvokeBindingAsync(request, ct);
            return true;
        }, ct);
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

        await TraceAsync(DeleteOperation, async activity =>
        {
            var request = BuildPathRequest(DeleteOperation, _options.BasePath, fileName, activity);
            AddDeleteMetadata(request.Metadata);
            await DaprClient.InvokeBindingAsync(request, ct);
            return true;
        }, ct);
    }

    /// <summary>Runs one binding operation inside its span, <c>{operation} {binding}</c>, and marks
    /// the span as failed when it throws.</summary>
    private async Task<T> TraceAsync<T>(string operation, Func<Activity?, Task<T>> invoke, CancellationToken ct)
    {
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity($"{operation} {_options.DaprBindingName}");
        activity?.SetTag("intropy.file.adapter", AdapterKind);
        activity?.SetTag("intropy.file.binding", _options.DaprBindingName);
        activity?.SetTag("intropy.file.operation", operation);

        try
        {
            return await invoke(activity);
        }
        catch (Exception e)
        {
            RecordFailure(activity, e, ct);
            throw;
        }
    }

    private BindingRequest BuildPathRequest(string operation, string basePath, string fileName, Activity? activity)
    {
        var path = CombinePath(basePath, fileName);
        activity?.SetTag("file.name", fileName);
        activity?.SetTag("file.path", path);
        return new BindingRequest(_options.DaprBindingName, operation) { Metadata = { [FileNameMetadataKey] = path } };
    }

    /// <summary>Marks the operation's span as failed. A cancellation the caller requested is a
    /// clean stop, not a failure of the operation, and leaves the span as it is.</summary>
    private static void RecordFailure(Activity? activity, Exception e, CancellationToken ct)
    {
        if (activity is null || (e is OperationCanceledException && ct.IsCancellationRequested))
            return;

        activity.AddException(e);
        activity.SetTag("error.type", e.GetType().FullName);
        activity.SetStatus(ActivityStatusCode.Error, e.Message);
    }

    private static List<string> FilterFiles(List<string> fileNames, Regex regex) =>
        fileNames.Where(fileName => regex.IsMatch(fileName)).ToList();
}
