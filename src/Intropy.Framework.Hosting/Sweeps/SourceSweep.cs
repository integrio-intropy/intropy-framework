using System.Runtime.ExceptionServices;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// The sweep of one source port, with every rule a framework sweep follows built in: a kind of
/// source derives from this and supplies only what is its own — listing its entries, creating the
/// item for an entry (reading it, and what it records in the context), completing a handled item,
/// and, optionally, validating the port. <see cref="FileSweeps.FileSweep"/> is one.
/// </summary>
/// <remarks>
/// <para>
/// What the sweep does, the same for every kind of source:
/// </para>
/// <list type="bullet">
/// <item>Nothing is listed when the host has already cancelled.</item>
/// <item>Entries are handled one after another, as they are listed; the host's cancellation is
/// checked before each, and the sweep stops after an interrupted (<see cref="SweepOutcome.Aborted"/>)
/// item. Entries not started stay in the source.</item>
/// <item>Each item is handled by a <see cref="SweepItemRunner"/>: its own scope and trace, its
/// logging scope and metrics, a failure or an unrequested cancellation isolated as a failure of
/// the item, and completion only after <see cref="SweepOutcome.Consumed"/> or
/// <see cref="SweepOutcome.Duplicate"/>.</item>
/// <item>Resolving an entry's identity (<see cref="IdOf"/>) and telemetry
/// (<see cref="TelemetryOf"/>) is part of creating its item: a throw, a missing identity, or an
/// unrequested cancellation fails only that item, which stays in the source; a host-requested
/// cancellation interrupts it. An entry without an identity is traced and logged as
/// <c>entry#{n}</c> (its 1-based position in the listing), for diagnostics only.</item>
/// <item>A listing failure is never an empty successful sweep: it propagates once the items
/// already started are recorded. A listing cancelled without a host cancellation fails the
/// sweep; one the host cancelled ends it with the outcomes recorded so far.</item>
/// <item>A run whose only failure is an unexpected scope-cleanup cancellation fails
/// (<see cref="SweepItemRunner.Finish"/>).</item>
/// </list>
/// <para>
/// A source that does not fit "list, then handle each entry" implements <see cref="ISourceSweep"/>
/// itself and handles each item with a <see cref="SweepItemRunner"/>.
/// </para>
/// </remarks>
/// <typeparam name="TEntry">What the source lists: a file entry, a message, a page of rows.</typeparam>
/// <typeparam name="TItem">The source's kind of item.</typeparam>
public abstract class SourceSweep<TEntry, TItem> : ISourceSweep where TItem : SweptItem
{
    private readonly ILogger _logger;

    /// <summary>Creates the sweep of <paramref name="port"/>.</summary>
    /// <param name="context">The component's root services, its name and its logging.</param>
    /// <param name="port">The source port.</param>
    protected SourceSweep(SourceSweepContext context, string port)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        Context = context;
        Port = port;
        _logger = context.LoggerFactory.CreateLogger(GetType());
    }

    /// <summary>The component's root services, its name and its logging.</summary>
    protected SourceSweepContext Context { get; }

    /// <inheritdoc />
    public string Port { get; }

    /// <inheritdoc />
    /// <remarks>By default, nothing to check.</remarks>
    public virtual Task ValidateAsync(CancellationToken ct) => Task.CompletedTask;

    /// <summary>Sweeps the port once.</summary>
    /// <param name="handle">Handles one item and reports its outcome. It must not complete the item
    /// itself: the sweep does, after it returns.</param>
    /// <param name="ct">The host's cancellation token.</param>
    /// <returns>Consumed items as <see cref="RunSummary.Processed"/>, duplicates as
    /// <see cref="RunSummary.Skipped"/>, and items left in the source as
    /// <see cref="RunSummary.Failed"/>. A cancelled sweep returns the counts recorded so far.</returns>
    /// <exception cref="InvalidOperationException">The listing was cancelled without a host
    /// cancellation, or the only failure was an unexpected cleanup cancellation. Other listing
    /// exceptions propagate.</exception>
    public async Task<RunSummary> SweepAsync(Func<TItem, CancellationToken, Task<SweepOutcome>> handle,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(handle);
        var runner = new SweepItemRunner(Context, Port);
        if (ct.IsCancellationRequested)
        {
            _logger.LogInformation("Cancellation was requested before the sweep of {SourcePort} of {Component} started; nothing to do",
                Port, Context.ComponentName);
            return runner.Finish();
        }

        var listed = 0;
        var entries = ListAsync(ct).GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                if (ct.IsCancellationRequested)
                {
                    // Items already completed or failed were real outcomes; the rest stay in the
                    // source for the next run.
                    _logger.LogInformation("Cancellation requested for {Component}; the sweep of {SourcePort} stops after {Listed} item(s)",
                        Context.ComponentName, Port, listed);
                    break;
                }

                if (!await NextAsync(entries, runner, ct))
                    break;

                listed++;
                var entry = entries.Current;
                var (id, telemetry, preparationFailure) = Prepare(entry, listed);
                var outcome = await runner.RunAsync(id, scope =>
                {
                    // A failed IdOf or TelemetryOf fails this item through the runner, like a
                    // failed CreateItem: an unrequested cancellation is a failure of the item, a
                    // host-requested one interrupts the sweep. The handler and completion never run.
                    preparationFailure?.Throw();
                    return CreateItem(entry, scope);
                }, handle, CompleteAsync, ct, telemetry);
                OnItemSwept(entry, outcome);
                if (outcome is SweepOutcome.Aborted)
                {
                    _logger.LogInformation("Sweep of {SourcePort} of {Component} was interrupted while processing {SourceItem}; it stays in the source for the next run",
                        Port, Context.ComponentName, id);
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // The host cancelled while the source was listing: the outcomes recorded so far stand.
            _logger.LogInformation("Cancellation was requested for {Component} while listing {SourcePort}; the outcomes recorded so far stand",
                Context.ComponentName, Port);
        }
        finally
        {
            await DisposeQuietlyAsync(entries);
        }

        var summary = runner.Summary;
        if (listed == 0)
            _logger.LogInformation("Nothing to sweep on {SourcePort} of {Component}", Port, Context.ComponentName);
        else
            _logger.LogInformation("Swept {Listed} item(s) from {SourcePort} of {Component}: {Processed} processed, {Skipped} skipped, {Failed} failed",
                listed, Port, Context.ComponentName, summary.Processed, summary.Skipped, summary.Failed);
        if (summary.Failed > 0)
            _logger.LogWarning("{Failed} item(s) from {SourcePort} failed for {Component}; exiting 1 so the scheduler records a failed run",
                summary.Failed, Port, Context.ComponentName);

        return runner.Finish();
    }

    Task<RunSummary> ISourceSweep.SweepAsync(Func<SweptItem, CancellationToken, Task<SweepOutcome>> handle,
        CancellationToken ct) => SweepAsync(handle, ct);

    /// <summary>Lists the port's entries, in the order they are handled. It may stream them (pages,
    /// for example): each entry is handled before the next is requested. A listing failure throws;
    /// it is never reported as an empty source.</summary>
    /// <param name="ct">The host's cancellation token.</param>
    protected abstract IAsyncEnumerable<TEntry> ListAsync(CancellationToken ct);

    /// <summary>The entry's identity in its source, for its trace, logs and errors: a file's name.
    /// A throw or a null or whitespace identity fails only this entry's item.</summary>
    /// <param name="entry">A listed entry.</param>
    protected abstract string IdOf(TEntry entry);

    /// <summary>Creates the item for <paramref name="entry"/> in its own scope: how it is read, and
    /// what it records in the component's context. A failure here fails the item and keeps it.</summary>
    /// <param name="entry">A listed entry.</param>
    /// <param name="scope">The item's own scope.</param>
    protected abstract TItem CreateItem(TEntry entry, IServiceProvider scope);

    /// <summary>Completes a handled item in its source: deletes, archives or acknowledges it. Called
    /// only after a <see cref="SweepOutcome.Consumed"/> or <see cref="SweepOutcome.Duplicate"/>
    /// outcome, and always awaited; a completion that throws keeps the item, which counts as failed.</summary>
    /// <param name="item">The handled item.</param>
    /// <param name="outcome"><see cref="SweepOutcome.Consumed"/> or <see cref="SweepOutcome.Duplicate"/>.</param>
    protected abstract Task CompleteAsync(TItem item, SweepOutcome outcome);

    /// <summary>What the source adds to the entry's span and logging scope. By default, nothing.
    /// It cannot replace the framework's own keys (the port and the item); a throw fails only this
    /// entry's item.</summary>
    /// <param name="entry">A listed entry.</param>
    protected virtual SweepItemTelemetry? TelemetryOf(TEntry entry) => null;

    /// <summary>Called with each entry's final outcome, after its trace has ended (for a metric of
    /// the source's own, for example). By default, nothing.</summary>
    /// <param name="entry">The entry.</param>
    /// <param name="outcome">Its final outcome.</param>
    protected virtual void OnItemSwept(TEntry entry, SweepOutcome outcome)
    {
    }

    /// <summary>Resolves the entry's identity and telemetry. A failure is captured, not thrown, so
    /// the runner fails only this item. When there is no identity, the item is traced and logged
    /// as <c>entry#{ordinal}</c>: a diagnostic identity only, never given to the handler or to
    /// completion.</summary>
    private (string Id, SweepItemTelemetry? Telemetry, ExceptionDispatchInfo? Failure) Prepare(TEntry entry, int ordinal)
    {
        string id;
        try
        {
            id = IdOf(entry);
        }
        catch (Exception e)
        {
            return ($"entry#{ordinal}", null, ExceptionDispatchInfo.Capture(e));
        }

        if (string.IsNullOrWhiteSpace(id))
            return ($"entry#{ordinal}", null, ExceptionDispatchInfo.Capture(new InvalidOperationException(
                $"Component '{Context.ComponentName}': source port '{Port}' gave entry #{ordinal} no identity (IdOf returned null or whitespace)")));

        try
        {
            return (id, TelemetryOf(entry), null);
        }
        catch (Exception e)
        {
            return (id, null, ExceptionDispatchInfo.Capture(e));
        }
    }

    private async Task<bool> NextAsync(IAsyncEnumerator<TEntry> entries, SweepItemRunner runner, CancellationToken ct)
    {
        try
        {
            return await entries.MoveNextAsync();
        }
        catch (OperationCanceledException e) when (!ct.IsCancellationRequested)
        {
            throw new InvalidOperationException(
                $"Component '{Context.ComponentName}': listing source port '{Port}' failed with an unexpected cancellation " +
                $"({e.Message}); recorded so far: {Describe(runner.Summary)}", e);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            _logger.LogError(e, "Listing {SourcePort} of {Component} failed; recorded so far: {Recorded}",
                Port, Context.ComponentName, Describe(runner.Summary));
            throw;
        }
    }

    private async ValueTask DisposeQuietlyAsync(IAsyncEnumerator<TEntry> entries)
    {
        try
        {
            await entries.DisposeAsync();
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Releasing the listing of {SourcePort} failed; the recorded outcomes stand", Port);
        }
    }

    private static string Describe(RunSummary summary) =>
        $"{summary.Processed} processed, {summary.Failed} failed, {summary.Skipped} skipped";
}
