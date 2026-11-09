using System.Diagnostics;
using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.StatefulSweeps;

/// <summary>
/// One sweep of a polled source (<see cref="IPolledSource"/>), independent of the component kind that
/// runs it: reads the source once into one message, hands it to a handler inside its own async scope,
/// and, for a source with a cursor, moves the cursor only after the handler reports the message
/// consumed or a duplicate. It is the polled counterpart of <see cref="FileSweep"/>: moving the cursor
/// is what deleting or archiving is for a file.
/// </summary>
/// <remarks>
/// <para>
/// The sweep owns the cursor protocol; the source only reads. The cursor is kept in an
/// <see cref="ICursorStore"/> (by default the Dapr state store the source names) as one record per
/// component and port, with a <c>committed</c> cursor and a <c>pending</c> upper bound:
/// </para>
/// <list type="number">
/// <item>When <c>pending</c> is set, a previous run read up to it and did not commit: read exactly
/// <c>(committed, pending]</c> again.</item>
/// <item>Otherwise read everything after <c>committed</c>; the source reports the new cursor.</item>
/// <item>Save it as <c>pending</c>, then hand the message over with the idempotency key
/// <c>{port}:{committed}:{pending}</c> in <see cref="SourceContextKeys.IdempotencyKey"/>.</item>
/// <item>Once it is handled, save <c>committed = pending</c> and clear <c>pending</c>.</item>
/// </list>
/// <para>
/// Pinning the upper bound before handing the message over is what makes a re-read after a crash
/// produce the same key: were the bound recomputed, new rows would move it, and the overlap would be
/// sent twice. A source that cannot re-read a bounded range (<see cref="CursorOptions.CanPin"/> false)
/// keeps no <c>pending</c> and gets plain at-least-once delivery.
/// </para>
/// <para>
/// A source without a cursor (a full export) reads everything on every run: there is nothing to
/// complete, no idempotency key, and a rerun sends a fresh export.
/// </para>
/// <para>
/// A read that does not move the cursor (an empty result the source still sends) is handed over with
/// a key unique to the run, so it is never taken for a duplicate, and saves nothing.
/// </para>
/// <para>
/// Delivery is at-least-once, as for files: a crash after the handler published and before the
/// cursor is committed means the next run sends the same range again. Cancellation is checked before
/// the read; an interrupted message never moves the cursor. Committing is always awaited to the end,
/// even when the host cancels. The sweep assumes one run of a component at a time, as a scheduled
/// job has.
/// </para>
/// <para>
/// Tracing: the read is its own trace, rooted in a <c>process {sourcePort}</c> span linked to the
/// span current when the sweep started (the job's), like a swept file.
/// </para>
/// </remarks>
/// <param name="provider">The root provider: the source, the cursor store and the message's scope
/// come from it.</param>
/// <param name="sourcePort">The source port: the key of the <see cref="IPolledSource"/> registered
/// for it.</param>
/// <param name="componentName">The component name, for the cursor's key, logs and errors.</param>
/// <param name="loggerFactory">The logger factory.</param>
public sealed class StatefulSweep(
    IServiceProvider provider,
    string sourcePort,
    string componentName,
    ILoggerFactory loggerFactory)
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<StatefulSweep>();

    /// <summary>The cursor record's key: one per component and port.</summary>
    internal string CursorKey => $"{componentName}:{sourcePort}";

    /// <summary>Sweeps the source once.</summary>
    /// <param name="handleItem">Processes the message inside its scope and reports the outcome. It must
    /// not move the cursor itself: the sweep does, after it returns.</param>
    /// <param name="ct">The host's cancellation token.</param>
    /// <returns>A handled message as <c>Processed</c>, a duplicate as <see cref="RunSummary.Skipped"/>,
    /// and a message that was not handled (or whose cursor could not be committed) as
    /// <see cref="RunSummary.Failed"/>; empty when there was nothing to send.</returns>
    /// <exception cref="InvalidOperationException">Reading the source or the cursor surfaced a
    /// cancellation the host did not request. Other read failures propagate: a failed read is never
    /// reported as an empty successful sweep.</exception>
    public async Task<RunSummary> SweepAsync(
        Func<SweptFile, CancellationToken, Task<FileOutcome>> handleItem, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(handleItem);
        if (ct.IsCancellationRequested)
        {
            _logger.LogInformation("Cancellation was requested before the sweep of {Component} started; nothing to do",
                componentName);
            return RunSummary.Empty;
        }

        var runActivity = Activity.Current;
        var activity = StartActivity(runActivity);
        try
        {
            using var logScope = _logger.BeginScope(new Dictionary<string, object> { ["SourcePort"] = sourcePort });
            return await SweepInActivityAsync(handleItem, activity, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _logger.LogInformation("Cancellation was requested for {Component} while reading source port {Port}; nothing was sent",
                componentName, sourcePort);
            return RunSummary.Empty;
        }
        catch (OperationCanceledException e)
        {
            RecordFailure(activity, e);
            throw new InvalidOperationException(
                $"Component '{componentName}': reading source port '{sourcePort}' failed with an unexpected cancellation ({e.Message})", e);
        }
        catch (Exception e)
        {
            RecordFailure(activity, e);
            throw;
        }
        finally
        {
            activity?.Dispose();
            Activity.Current = runActivity;
        }
    }

    private async Task<RunSummary> SweepInActivityAsync(
        Func<SweptFile, CancellationToken, Task<FileOutcome>> handleItem, Activity? activity, CancellationToken ct)
    {
        var source = provider.GetRequiredKeyedService<IPolledSource>(sourcePort);
        var cursor = source.Cursor;

        ICursorStore? store = null;
        var state = new CursorState(null, null);
        if (cursor is not null)
        {
            store = ResolveStore(cursor.StoreName);
            state = await store.GetAsync(CursorKey, ct) ?? new CursorState(cursor.Initial, null);
        }

        var pinned = cursor is { CanPin: true } && state.Pending is not null;
        var range = cursor is null ? CursorRange.All : new CursorRange(state.Committed, pinned ? state.Pending : null);
        activity?.SetTag("intropy.cursor.from", range.From);
        if (pinned)
        {
            activity?.SetTag("intropy.cursor.pinned", true);
            _logger.LogInformation(
                "Source port {Port} has a pending read that was not committed; reading exactly ({From}, {To}] again",
                sourcePort, range.From, range.To);
        }

        var read = await source.ReadAsync(range, ct);
        if (read is null)
        {
            if (pinned && store is not null)
            {
                // The pinned range no longer yields a message: there is nothing left to deliver from it,
                // and keeping it pending would read the same nothing on every run.
                await store.SaveAsync(CursorKey, new CursorState(state.Pending, null), CancellationToken.None);
                _logger.LogInformation("The pending read of {Port} has nothing to send any more; committed its bound {To}",
                    sourcePort, state.Pending);
            }
            else
            {
                _logger.LogInformation("Source port {Port} of {Component} has nothing to send", sourcePort, componentName);
            }

            RecordOutcome(activity, "empty");
            return RunSummary.Empty;
        }

        var to = pinned ? state.Pending : read.Cursor;
        var moves = cursor is not null && to is not null && !string.Equals(to, state.Committed, StringComparison.Ordinal);
        activity?.SetTag("intropy.cursor.to", to);

        if (moves && cursor!.CanPin && !pinned)
            await store!.SaveAsync(CursorKey, new CursorState(state.Committed, to), ct);

        var metadata = new Dictionary<string, string>(read.Metadata);
        if (cursor is not null)
            metadata[SourceContextKeys.IdempotencyKey] = moves
                ? $"{sourcePort}:{state.Committed}:{to}"
                // Nothing new, so nothing to deduplicate: unique to this run.
                : $"{sourcePort}:{state.Committed}:{state.Committed}:{Guid.NewGuid():N}";

        var name = cursor is null ? sourcePort : $"{sourcePort} ({state.Committed}, {to ?? state.Committed}]";
        var outcome = await HandleInScopeAsync(name, read.Content, metadata, handleItem, activity, ct);

        if (outcome is FileOutcome.Consumed or FileOutcome.Duplicate && moves)
            outcome = await CommitAsync(store!, to!, activity, outcome);

        RecordOutcome(activity, HostingMetrics.OutcomeName(outcome));
        HostingMetrics.RecordPolledRead(componentName, sourcePort, outcome);
        return outcome switch
        {
            FileOutcome.Consumed => new RunSummary(1, 0, 0),
            FileOutcome.Duplicate => new RunSummary(0, 0, 1),
            FileOutcome.Aborted => RunSummary.Empty,
            _ => new RunSummary(0, 1, 0)
        };
    }

    private async Task<FileOutcome> HandleInScopeAsync(string name, byte[] content,
        IReadOnlyDictionary<string, string> metadata, Func<SweptFile, CancellationToken, Task<FileOutcome>> handleItem,
        Activity? activity, CancellationToken ct)
    {
        await using var scope = provider.CreateAsyncScope();
        try
        {
            var outcome = await handleItem(new SweptFile(name, sourcePort, content, metadata, scope.ServiceProvider), ct);
            if (outcome is not FileOutcome.Aborted || ct.IsCancellationRequested)
                return outcome;

            _logger.LogWarning("Processing {Item} was aborted without a host cancellation; the cursor stays", name);
            return FileOutcome.Failed;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return FileOutcome.Aborted;
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Processing {Item} failed unexpectedly; the cursor stays for the next run", name);
            RecordFailure(activity, e);
            return FileOutcome.Failed;
        }
    }

    /// <returns><paramref name="outcome"/> once the cursor is committed; <see cref="FileOutcome.Failed"/>
    /// when committing failed. The message is already handled, so the next run reads the same range
    /// again (it is still pending) and the idempotency key absorbs the duplicate.</returns>
    private async Task<FileOutcome> CommitAsync(ICursorStore store, string to, Activity? activity, FileOutcome outcome)
    {
        try
        {
            // Committing finishes an already handled message and is always awaited.
            await store.SaveAsync(CursorKey, new CursorState(to, null), CancellationToken.None);
            _logger.LogInformation("Committed the cursor of source port {Port} at {Cursor}", sourcePort, to);
            return outcome;
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Could not commit the cursor of source port {Port} at {Cursor}; the next run reads the same range again",
                sourcePort, to);
            RecordFailure(activity, e);
            return FileOutcome.Failed;
        }
    }

    private ICursorStore ResolveStore(string storeName) =>
        provider.GetKeyedService<ICursorStore>(storeName)
        ?? new DaprCursorStore(provider.GetService<DaprClient>() ?? throw new InvalidOperationException(
                $"Component '{componentName}': source port '{sourcePort}' keeps its cursor in the state store " +
                $"'{storeName}', but no DaprClient is registered."),
            storeName);

    private Activity? StartActivity(Activity? runActivity)
    {
        Activity.Current = null;
        var activity = ActivitySourceProvider.ActivitySource.StartActivity(
            $"process {sourcePort}",
            ActivityKind.Internal,
            parentContext: default,
            links: runActivity is null ? null : [new ActivityLink(runActivity.Context)]);
        activity?.SetTag("intropy.source.port", sourcePort);
        return activity;
    }

    private static void RecordOutcome(Activity? activity, string outcome)
    {
        if (activity is null)
            return;

        activity.SetTag("intropy.sweep.outcome", outcome);
        if (outcome == HostingMetrics.OutcomeName(FileOutcome.Failed) && activity.Status == ActivityStatusCode.Unset)
            activity.SetStatus(ActivityStatusCode.Error, "The message was not handled; the cursor stays for the next run");
    }

    private static void RecordFailure(Activity? activity, Exception e)
    {
        activity?.AddException(e);
        activity?.SetStatus(ActivityStatusCode.Error, e.Message);
    }
}
