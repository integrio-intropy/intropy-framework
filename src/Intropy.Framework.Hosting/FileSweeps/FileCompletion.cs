using Intropy.Framework.Adapters.File;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// What happens to a source file once its handler reports it <see cref="FileOutcome.Consumed"/>
/// or <see cref="FileOutcome.Duplicate"/>. Completion only ever runs after the handler returns,
/// so a source is never completed before its content has been handled.
/// </summary>
/// <remarks>
/// Use <see cref="Delete"/> or <see cref="Archive"/>, or derive a custom mode. A completion that
/// throws keeps the file in the source: it counts as failed and the next run handles it again.
/// Completion is always awaited to the end, even when the host cancels the sweep.
/// </remarks>
public abstract class FileCompletion
{
    /// <summary>Delete the source.</summary>
    public static FileCompletion Delete { get; } = new DeleteCompletion();

    /// <summary>Copy the source to <paramref name="basePath"/> through the same adapter, then
    /// delete it. A failed copy keeps the source, so the next run retries it.</summary>
    /// <remarks>On an <see cref="SmbAdapter"/> the file is moved with the binding's rename instead:
    /// atomic and without rewriting the content. A rename never overwrites, so when the archive
    /// already holds a file of that name, the source is archived under a timestamped name
    /// (<c>order_20261026T101500123Z.xml</c>) rather than kept in the source to be handled again
    /// on every run.</remarks>
    /// <param name="basePath">The archive base path, used as the adapter's base path override.</param>
    public static FileCompletion Archive(string basePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(basePath);
        return new ArchiveCompletion(basePath);
    }

    /// <summary>Completes one handled file.</summary>
    /// <param name="source">The adapter the file was listed from.</param>
    /// <param name="file">The file, with its content (read once and kept) and its own scope.</param>
    /// <param name="outcome"><see cref="FileOutcome.Consumed"/> or
    /// <see cref="FileOutcome.Duplicate"/>.</param>
    public abstract Task CompleteAsync(IFileAdapter source, SweptFile file, FileOutcome outcome);

    private sealed class DeleteCompletion : FileCompletion
    {
        public override Task CompleteAsync(IFileAdapter source, SweptFile file, FileOutcome outcome) =>
            source.DeleteAsync(file.Name);

        public override string ToString() => "delete";
    }

    private sealed class ArchiveCompletion(string basePath) : FileCompletion
    {
        public override async Task CompleteAsync(IFileAdapter source, SweptFile file, FileOutcome outcome)
        {
            if (source is SmbAdapter smb)
            {
                try
                {
                    await smb.MoveAsync(file.Name, basePath);
                }
                catch (Exception)
                {
                    // Most likely the archive already has the name. If the cause was anything
                    // else, this fails too and the file stays for the next run.
                    await smb.MoveAsync(file.Name, basePath, Timestamped(file.Name, DateTimeOffset.UtcNow));
                }
                return;
            }

            await source.WriteAsync(file.Name, await file.ReadAsync(), basePath);
            await source.DeleteAsync(file.Name);
        }

        private static string Timestamped(string fileName, DateTimeOffset now)
        {
            var extension = Path.GetExtension(fileName);
            return $"{fileName[..^extension.Length]}_{now.UtcDateTime:yyyyMMdd'T'HHmmssfff'Z'}{extension}";
        }

        public override string ToString() => $"archive to '{basePath}'";
    }
}
