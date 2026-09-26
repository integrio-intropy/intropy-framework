using Intropy.Framework.Adapters.Common;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.RunToCompletion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Sweep;

/// <summary>
/// One sequential sweep of a file source, independent of the component kind that runs it: lists
/// the source through its keyed <see cref="IFileAdapter"/>, hands
/// each file to a handler inside its own async scope, and completes the file (its
/// <see cref="SweepCompletion"/>) only after the handler reports it consumed or a duplicate. Failed
/// files stay for the next run.
/// </summary>
/// <remarks>
/// <para>
/// The adapter is a singleton — a sweep reads exactly one source — resolved once, from the root
/// provider. Each file gets a fresh scope, disposed after its outcome is recorded.
/// </para>
/// <para>
/// Cancellation: checked before listing and before each new file; in-flight work (completion
/// included) is always awaited, and an interrupted file is never completed.
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
    SweepCompletion completion,
    ILoggerFactory loggerFactory)
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<FileSweep>();

    /// <summary>Sweeps the source once.</summary>
    /// <param name="handleFile">Processes one file inside its scope and reports the outcome. It
    /// must not complete the source itself: the sweep does, after it returns.</param>
    /// <param name="ct">The host's cancellation token.</param>
    /// <returns>Consumed files as <c>Processed</c>, duplicates as <see cref="JobRunSummary.Skipped"/>,
    /// and files left in place as <see cref="JobRunSummary.Failed"/>. A cancelled sweep returns the
    /// counts recorded so far.</returns>
    /// <exception cref="InvalidOperationException">Adapter construction or listing surfaced a
    /// cancellation the host did not request, or the only failure was an unexpected cleanup
    /// cancellation. Other listing exceptions propagate: a listing failure is never reported as
    /// an empty successful sweep.</exception>
    public async Task<JobRunSummary> SweepAsync(
        Func<SweptFile, CancellationToken, Task<SweepOutcome>> handleFile, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(handleFile);
        var state = new SweepState();

        if (ct.IsCancellationRequested)
        {
            _logger.LogInformation("Cancellation was requested before the sweep of {Component} started; nothing to do",
                componentName);
            return JobRunSummary.Empty;
        }

        var processed = 0;
        var failed = 0;
        var skipped = 0;
        var summary = JobRunSummary.Empty;
        try
        {
            var source = provider.GetRequiredKeyedService<IFileAdapter>(sourcePort);

            // Adapter construction may have cancelled the host token without throwing;
            // re-check before starting source I/O.
            if (ct.IsCancellationRequested)
            {
                _logger.LogInformation("Cancellation was requested for {Component} while preparing the source adapter; nothing to do",
                    componentName);
                return JobRunSummary.Empty;
            }

            List<FileEntry> files;
            try
            {
                files = await source.ListAsync(ct);
            }
            catch (OperationCanceledException e) when (!ct.IsCancellationRequested)
            {
                throw new InvalidOperationException(
                    $"Component '{componentName}': listing the source failed with an unexpected cancellation ({e.Message})", e);
            }

            if (files.Count == 0)
            {
                _logger.LogInformation("Inbound is empty for {Component}; nothing to do", componentName);
            }
            else
            {
                _logger.LogInformation("Swept {Count} file(s) from the source of {Component}", files.Count, componentName);

                foreach (var file in files)
                {
                    if (ct.IsCancellationRequested)
                    {
                        // Files already completed or failed were real outcomes; the rest stay in
                        // the source for the next run.
                        _logger.LogInformation("Cancellation requested for {Component}; {Remaining} listed file(s) were not started",
                            componentName, files.Count - processed - failed - skipped);
                        break;
                    }

                    var outcome = await ProcessFileAsync(state, source, file.Name, handleFile, ct);
                    if (outcome is SweepOutcome.Aborted)
                    {
                        _logger.LogInformation("Sweep of {Component} was interrupted while processing {FileName}; the file is retained for the next run",
                            componentName, file.Name);
                        break;
                    }

                    switch (outcome)
                    {
                        case SweepOutcome.Consumed:
                            processed++;
                            break;
                        case SweepOutcome.Duplicate:
                            skipped++;
                            break;
                        default:
                            failed++;
                            break;
                    }
                }

                if (failed > 0)
                    _logger.LogWarning("{Failed} of {Total} file(s) failed for {Component}; exiting 1 so the scheduler records a failed run",
                        failed, files.Count, componentName);

                summary = new JobRunSummary(processed, failed, skipped);
            }
        }
        catch (OperationCanceledException e) when (!ct.IsCancellationRequested)
        {
            throw new InvalidOperationException(
                $"Component '{componentName}': preparing the source adapter failed with an unexpected cancellation ({e.Message})", e);
        }
        catch (OperationCanceledException)
        {
            // A host-requested cancellation during preparation or listing: nothing was processed.
            _logger.LogInformation("Cancellation was requested for {Component} while preparing the source adapter; nothing to do",
                componentName);
        }

        ThrowIfUnexpectedCleanupCancellation(state, summary);
        return summary;
    }

    /// <summary>The per-run sweep state.</summary>
    private sealed class SweepState
    {
        /// <summary>Number of scope cleanups in the current sweep that failed with a cancellation
        /// the host never requested. Single-threaded by the sequential loop.</summary>
        internal int UnexpectedCleanupCancellations;
    }

    /// <returns>The file's final outcome: the handler's outcome after completion, or
    /// <see cref="SweepOutcome.Failed"/> when the scope, the handler, or completion failed. Only
    /// a host-requested interruption returns <see cref="SweepOutcome.Aborted"/>.</returns>
    private async Task<SweepOutcome> ProcessFileAsync(
        SweepState state,
        IFileAdapter source,
        string fileName,
        Func<SweptFile, CancellationToken, Task<SweepOutcome>> handleFile,
        CancellationToken ct)
    {
        AsyncServiceScope? scope = null;
        try
        {
            SweptFile file;
            SweepOutcome outcome;
            try
            {
                scope = provider.CreateAsyncScope();
                file = new SweptFile(fileName, source, scope.Value.ServiceProvider);
                outcome = await handleFile(file, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return SweepOutcome.Aborted;
            }
            catch (OperationCanceledException e)
            {
                _logger.LogWarning(e, "Processing {FileName} was unexpectedly cancelled without a host cancellation; leaving it for the next run",
                    fileName);
                return SweepOutcome.Failed;
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "Processing {FileName} failed unexpectedly; leaving it for the next run", fileName);
                return SweepOutcome.Failed;
            }

            switch (outcome)
            {
                case SweepOutcome.Consumed or SweepOutcome.Duplicate:
                    return await CompleteAsync(source, file, outcome);
                case SweepOutcome.Aborted when ct.IsCancellationRequested:
                    return SweepOutcome.Aborted;
                case SweepOutcome.Aborted:
                    _logger.LogWarning("Processing {FileName} was aborted without a host cancellation; leaving it for the next run",
                        fileName);
                    return SweepOutcome.Failed;
                default:
                    return SweepOutcome.Failed;
            }
        }
        finally
        {
            await DisposeScopeQuietlyAsync(state, scope, fileName, ct);
        }
    }

    /// <returns><paramref name="outcome"/> once the source is completed;
    /// <see cref="SweepOutcome.Failed"/> when completion failed and the source stays. The handler
    /// already published, so the next run handles the file again; idempotency, where the
    /// component has it, cancels the duplicate.</returns>
    private async Task<SweepOutcome> CompleteAsync(
        IFileAdapter source, SweptFile file, SweepOutcome outcome)
    {
        try
        {
            // Completion finishes an already started file and is always awaited.
            await completion.CompleteAsync(source, file, outcome);
            return outcome;
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Could not complete {FileName} ({Completion}) after processing; the next run handles it again",
                file.Name, completion);
            return SweepOutcome.Failed;
        }
    }

    /// <summary>Disposes a file scope without letting a cleanup failure escape: the file's
    /// outcome is already recorded. Unexpected cancellations are counted so an otherwise clean run
    /// is still escalated.</summary>
    private async ValueTask DisposeScopeQuietlyAsync(SweepState state, AsyncServiceScope? scope, string fileName,
        CancellationToken ct)
    {
        if (scope is null)
            return;

        try
        {
            await scope.Value.DisposeAsync();
        }
        catch (OperationCanceledException e) when (!ct.IsCancellationRequested)
        {
            state.UnexpectedCleanupCancellations++;
            _logger.LogWarning(e, "Cleaning up the processing scope failed with an unexpected cancellation for {FileName}; the recorded outcomes stand",
                fileName);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Cleaning up the processing scope was interrupted by the host cancellation for {FileName}; the recorded outcomes stand",
                fileName);
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Cleaning up the processing scope failed for {FileName}; the recorded outcomes stand", fileName);
        }
    }

    /// <summary>Escalates a run whose only failure was an unexpected cleanup cancellation; the
    /// recorded outcomes are part of the error.</summary>
    private void ThrowIfUnexpectedCleanupCancellation(SweepState state, JobRunSummary recorded)
    {
        if (state.UnexpectedCleanupCancellations == 0 || recorded.Failed > 0)
            return;

        throw new InvalidOperationException(
            $"Component '{componentName}': {state.UnexpectedCleanupCancellations} scope cleanup(s) failed with an unexpected cancellation " +
            $"the host did not request. The recorded outcome stands: {recorded.Processed} processed, " +
            $"{recorded.Failed} failed, {recorded.Skipped} skipped — but a run with unexpected cancellations is not a clean run.");
    }
}
