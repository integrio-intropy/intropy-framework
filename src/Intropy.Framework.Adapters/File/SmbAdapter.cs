using Dapr.Client;
using Intropy.Framework.Adapters.Common;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Adapter for Windows file shares through the Intropy SMB binding (<c>bindings.smb</c>), which
/// follows the SFTP binding's operations and adds what SMB can guarantee.
/// </summary>
/// <remarks>
/// <para>
/// Reads are exclusive: a file someone else still has open is refused rather than read half
/// written, and a sweep leaves it for the next run.
/// </para>
/// <para>
/// The binding never overwrites. A write refused because the file is already there with the same
/// content counts as done, so a retried write whose first attempt landed succeeds; different
/// content is still an error.
/// </para>
/// </remarks>
/// <param name="daprClient">The Dapr client the binding calls go through.</param>
/// <param name="options">The adapter's binding name, base path, and optional listing regex.</param>
public class SmbAdapter(DaprClient daprClient, FileAdapterOptions options)
    : DaprBindingFileAdapter(daprClient, options)
{
    internal const string RenameOperation = "rename";
    private const string NewFileNameKey = "newFileName";
    private const string ExclusiveKey = "exclusive";

    /// <inheritdoc/>
    internal override string AdapterKind => "smb";

    /// <summary>bindings.smb carries the file path under <c>fileName</c>, as bindings.sftp does.</summary>
    internal override string FileNameMetadataKey => "fileName";

    /// <summary>Joins with <see cref="Path.Combine(string, string)"/>, as <see cref="SftpAdapter"/> does.</summary>
    internal override string CombinePath(string basePath, string fileName) =>
        Path.Combine(basePath, fileName);

    internal override List<FileEntry> ParseListResponse(BindingResponse response) =>
        SftpAdapter.ParseSftpListing(response);

    /// <summary>Refused while someone else has the file open, so a half-written file is never read.</summary>
    internal override void AddGetMetadata(Dictionary<string, string> metadata) =>
        metadata[ExclusiveKey] = "true";

    /// <summary>Reads the file back: the same bytes mean an earlier attempt of this write landed.
    /// Any failure to read it means no, and the write's own error stands. Not exclusive: the
    /// partner may already be reading the file we wrote.</summary>
    internal override async Task<bool> IsAlreadyWrittenAsync(string path, byte[] content, CancellationToken ct)
    {
        try
        {
            var request = new BindingRequest(Options.DaprBindingName, GetOperation)
                { Metadata = { [FileNameMetadataKey] = path } };
            var response = await DaprClient.InvokeBindingAsync(request, ct);
            return response.Data.Span.SequenceEqual(content);
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>
    /// Moves a file under <paramref name="destinationBasePath"/> with the binding's rename: atomic,
    /// on the server, without reading or rewriting the content. Missing folders are created.
    /// </summary>
    /// <param name="fileName">The file's name under the configured base path.</param>
    /// <param name="destinationBasePath">The base path to move it under.</param>
    /// <param name="newFileName">The name to give it there; by default its current name.</param>
    /// <param name="ct">A cancellation token that can cancel the move.</param>
    /// <exception cref="Exception">The binding's exception, including when the destination already
    /// exists: a rename never overwrites.</exception>
    public async Task MoveAsync(string fileName, string destinationBasePath, string? newFileName = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(destinationBasePath);

        await TraceAsync(RenameOperation, async activity =>
        {
            var request = BuildPathRequest(RenameOperation, Options.BasePath, fileName, activity);
            var newPath = CombinePath(destinationBasePath, newFileName ?? fileName);
            request.Metadata[NewFileNameKey] = newPath;
            activity?.SetTag("intropy.file.new_path", newPath);
            await DaprClient.InvokeBindingAsync(request, ct);
            return true;
        }, ct);
    }
}
