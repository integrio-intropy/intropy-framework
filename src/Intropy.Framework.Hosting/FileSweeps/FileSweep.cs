using System.Runtime.CompilerServices;
using Intropy.Framework.Adapters.Common;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.StatefulSweeps;
using Intropy.Framework.Hosting.Sweeps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// One sequential sweep of a file source, independent of the component kind that runs it: lists
/// the source through its keyed <see cref="IFileAdapter"/>, hands
/// each file to a handler inside its own async scope, and completes the file (its
/// <see cref="FileCompletion"/>) only after the handler reports it consumed or a duplicate. Failed
/// files stay for the next run.
/// </summary>
/// <remarks>
/// <para>
/// The adapter is a singleton — a sweep reads exactly one source — resolved once, from the root
/// provider. Each file gets a fresh scope, disposed after its outcome is recorded.
/// </para>
/// <para>
/// Cancellation: checked before listing and before each new file; in-flight work (completion
/// included) is always awaited, and an interrupted file is never completed. These rules, and the
/// ones below, are every framework sweep's (<see cref="SourceSweep{TEntry, TItem}"/>): this sweep
/// supplies only the file-specific parts — listing through the adapter, reading a file, and its
/// <see cref="FileCompletion"/>.
/// </para>
/// <para>
/// No false success: every <see cref="OperationCanceledException"/> the host did not request is
/// normalized into a failure, so a caller that treats cancellation as a clean stop never
/// mistakes it for one. Cleanup failures are logged and never mask recorded outcomes, but an
/// unexpected cleanup cancellation on an otherwise clean run is escalated to a job failure.
/// </para>
/// <para>
/// Delivery is at-least-once: a crash after the handler published but before completion means
/// the next run handles the file again.
/// </para>
/// <para>
/// Tracing: each file is its own trace, rooted in a <c>process {sourcePort}</c> span linked to
/// the span current when the sweep started (the job's), so a large sweep is not one oversized
/// trace. The file's read, its handling and its completion are all in that trace, and so is
/// every log written while the file is processed, which also carries the file name as a logging
/// scope. Each file is handled by a <see cref="SweepItemRunner"/>, as every framework sweep's
/// items are; the span is also tagged <c>file.name</c>, and each file is counted on
/// <c>intropy.sweep.files</c> as well as <c>intropy.sweep.items</c>. A handler continues the file's trace rather than detaching from it, so the trace can
/// continue downstream (through the queue, for a Transactional Integration).
/// </para>
/// </remarks>
/// <param name="provider">The root provider: the source adapter and every file scope come from it.</param>
/// <param name="sourcePort">The source port: the key of the <see cref="IFileAdapter"/> registered for it.</param>
/// <param name="componentName">The component name, for logs and errors.</param>
/// <param name="completion">What happens to a consumed or duplicate source file.</param>
/// <param name="loggerFactory">The logger factory.</param>
public sealed class FileSweep(
    IServiceProvider provider,
    string sourcePort,
    string componentName,
    FileCompletion completion,
    ILoggerFactory loggerFactory)
    : SourceSweep<FileEntry, SweptFile>(new SourceSweepContext(provider, componentName, loggerFactory), sourcePort)
{
    private readonly FileCompletion _completion = completion ?? throw new ArgumentNullException(nameof(completion));

    /// <summary>Checks that the port's <see cref="IFileAdapter"/> is registered, and that no source of
    /// another kind is: a port declared by name alone is a file port.</summary>
    /// <param name="ct">The host's cancellation token.</param>
    /// <exception cref="InvalidOperationException">No adapter is registered for the port, or a polled
    /// source is.</exception>
    public override Task ValidateAsync(CancellationToken ct = default)
    {
        if (provider.GetKeyedService<IPolledSource>(Port) is not null)
            throw new InvalidOperationException(
                $"Component '{Context.ComponentName}': source port '{Port}' is declared as a file port, but a polled " +
                $"source (IPolledSource) is registered for it. A port declared by name alone (Port(\"{Port}\"), " +
                $"SourcePort = \"{Port}\") is a file port; declare it as new PolledSourcePort(\"{Port}\") (Port(...), " +
                "or a definition's Source).");
        _ = provider.GetKeyedService<IFileAdapter>(Port) ?? throw new InvalidOperationException(
            $"Component '{Context.ComponentName}': no IFileAdapter is registered for the source port '{Port}'.");
        return Task.CompletedTask;
    }

    /// <summary>Lists the source through its adapter, resolved from the root provider.</summary>
    protected override async IAsyncEnumerable<FileEntry> ListAsync([EnumeratorCancellation] CancellationToken ct)
    {
        var source = Source;
        // Adapter construction may have cancelled the host token without throwing; re-check
        // before starting source I/O.
        if (ct.IsCancellationRequested)
            yield break;

        foreach (var file in await source.ListAsync(ct))
            yield return file;
    }

    /// <inheritdoc />
    protected override string IdOf(FileEntry entry) => entry.Name;

    /// <inheritdoc />
    protected override SweptFile CreateItem(FileEntry entry, IServiceProvider scope) =>
        new(entry.Name, Port, Source, scope);

    /// <summary>Completes the file with the sweep's <see cref="FileCompletion"/>.</summary>
    protected override Task CompleteAsync(SweptFile item, SweepOutcome outcome) =>
        _completion.CompleteAsync(Source, item, outcome);

    /// <summary>The file's name, as the span's <c>file.name</c> and the logging scope's
    /// <c>FileName</c>.</summary>
    protected override SweepItemTelemetry TelemetryOf(FileEntry entry) => new()
    {
        SpanTags = new Dictionary<string, object?> { ["file.name"] = entry.Name },
        LogScope = new Dictionary<string, object> { ["FileName"] = entry.Name },
    };

    /// <summary>Counts the file on <c>intropy.sweep.files</c>.</summary>
    protected override void OnItemSwept(FileEntry entry, SweepOutcome outcome) =>
        HostingMetrics.RecordSweptFile(Context.ComponentName, Port, outcome);

    // The adapter is a singleton: resolving it again for each file returns the one listed from.
    private IFileAdapter Source => provider.GetRequiredKeyedService<IFileAdapter>(Port);
}
