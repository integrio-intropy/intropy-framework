using Intropy.Framework.Adapters.File;

namespace Intropy.Framework.Hosting.Sweep;

/// <summary>
/// What happens to a source file once its handler reports it <see cref="SweepOutcome.Consumed"/>
/// or <see cref="SweepOutcome.Duplicate"/>. Completion only ever runs after the handler returns,
/// so a source is never completed before its content has been handled.
/// </summary>
/// <remarks>
/// Use <see cref="Delete"/> or <see cref="Archive"/>, or derive a custom mode. A completion that
/// throws keeps the file in the source: it counts as failed and the next run handles it again.
/// Completion is always awaited to the end, even when the host cancels the sweep.
/// </remarks>
public abstract class SweepCompletion
{
    /// <summary>Delete the source.</summary>
    public static SweepCompletion Delete { get; } = new DeleteCompletion();

    /// <summary>Copy the source to <paramref name="basePath"/> through the same adapter, then
    /// delete it. A failed copy keeps the source, so the next run retries it.</summary>
    /// <param name="basePath">The archive base path, used as the adapter's base path override.</param>
    public static SweepCompletion Archive(string basePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(basePath);
        return new ArchiveCompletion(basePath);
    }

    /// <summary>Completes one handled file.</summary>
    /// <param name="source">The adapter the file was listed from.</param>
    /// <param name="file">The file, with its content (read once and kept) and its own scope.</param>
    /// <param name="outcome"><see cref="SweepOutcome.Consumed"/> or
    /// <see cref="SweepOutcome.Duplicate"/>.</param>
    public abstract Task CompleteAsync(IFileAdapter source, SweptFile file, SweepOutcome outcome);

    private sealed class DeleteCompletion : SweepCompletion
    {
        public override Task CompleteAsync(IFileAdapter source, SweptFile file, SweepOutcome outcome) =>
            source.DeleteAsync(file.Name);

        public override string ToString() => "delete";
    }

    private sealed class ArchiveCompletion(string basePath) : SweepCompletion
    {
        public override async Task CompleteAsync(IFileAdapter source, SweptFile file, SweepOutcome outcome)
        {
            await source.WriteAsync(file.Name, await file.ReadAsync(), basePath);
            await source.DeleteAsync(file.Name);
        }

        public override string ToString() => $"archive to '{basePath}'";
    }
}
