using System.Diagnostics;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// Handles the items of one run's sweep of one source port the way every framework sweep does,
/// whatever kind of source the items come from: each item in its own scope and its own trace, its
/// handler's outcome mapped and counted, and the item completed in its source only after the
/// handler reports it <see cref="SweepOutcome.Consumed"/> or a <see cref="SweepOutcome.Duplicate"/>.
/// <see cref="SourceSweep{TEntry, TItem}"/> runs one for each run; a source that implements
/// <see cref="ISourceSweep"/> directly runs its own.
/// </summary>
/// <remarks>
/// <para>
/// Create one runner per run and handle the run's items one after another. Between items, the
/// sweep checks the host's cancellation and stops after an <see cref="SweepOutcome.Aborted"/>
/// item; after the last, it returns <see cref="Finish"/>.
/// </para>
/// <para>
/// No false success: an <see cref="OperationCanceledException"/> the host did not request is a
/// failure of the item, never an interruption. An item that is interrupted, fails, or throws is
/// never completed and stays in its source for the next run. Completion is always awaited, even
/// when the host cancels, and a completion that throws keeps the item (it counts as failed).
/// Delivery is at-least-once: a crash after the handler published but before completion means the
/// next run handles the item again.
/// </para>
/// <para>
/// Tracing: each item is its own trace, rooted in a <c>process {port}</c> span linked to the span
/// current when the item started (the job's), tagged with the port, the item, and its outcome, and
/// marked as an error when the item stays. Every log written while the item is processed is in that
/// trace and carries the item and the port as a logging scope. Metrics: one
/// <c>intropy.sweep.items</c> count per item, by component, port and outcome.
/// </para>
/// </remarks>
public sealed class SweepItemRunner
{
    private static readonly IReadOnlyDictionary<string, object?> NoTags = new Dictionary<string, object?>();
    private static readonly IReadOnlyDictionary<string, object> NoScope = new Dictionary<string, object>();

    private readonly SourceSweepContext _context;
    private readonly string _port;
    private readonly ILogger _logger;
    private int _processed;
    private int _failed;
    private int _skipped;
    private int _unexpectedCleanupCancellations;

    /// <summary>Creates the runner for one run's sweep of <paramref name="port"/>.</summary>
    /// <param name="context">The component's root services, its name and its logging.</param>
    /// <param name="port">The source port the items are swept from.</param>
    public SweepItemRunner(SourceSweepContext context, string port)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        _context = context;
        _port = port;
        _logger = context.LoggerFactory.CreateLogger<SweepItemRunner>();
    }

    /// <summary>The outcomes recorded so far: consumed items as <see cref="RunSummary.Processed"/>,
    /// duplicates as <see cref="RunSummary.Skipped"/>, and items left in their source as
    /// <see cref="RunSummary.Failed"/>. Interrupted items are not counted.</summary>
    public RunSummary Summary => new(_processed, _failed, _skipped);

    /// <summary>Handles one item: creates it in its own scope, runs <paramref name="handle"/>,
    /// and completes it after a consumed or duplicate outcome.</summary>
    /// <typeparam name="TItem">The source's kind of item.</typeparam>
    /// <param name="itemId">The item's identity in its source, for its trace and logs before it
    /// is created.</param>
    /// <param name="createItem">Creates the item in its scope.</param>
    /// <param name="handle">Handles the item and reports its outcome; the component's.</param>
    /// <param name="complete">Completes a consumed or duplicate item in its source; the source's.</param>
    /// <param name="ct">The host's cancellation token.</param>
    /// <param name="telemetry">What the source adds to the item's span and logging scope.</param>
    /// <returns>The item's final outcome: the handler's after completion, or
    /// <see cref="SweepOutcome.Failed"/> when creating, handling or completing it failed. Only a
    /// host-requested interruption returns <see cref="SweepOutcome.Aborted"/>.</returns>
    public async Task<SweepOutcome> RunAsync<TItem>(
        string itemId,
        Func<IServiceProvider, TItem> createItem,
        Func<TItem, CancellationToken, Task<SweepOutcome>> handle,
        Func<TItem, SweepOutcome, Task> complete,
        CancellationToken ct,
        SweepItemTelemetry? telemetry = null) where TItem : SweptItem
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(itemId);
        ArgumentNullException.ThrowIfNull(createItem);
        ArgumentNullException.ThrowIfNull(handle);
        ArgumentNullException.ThrowIfNull(complete);

        var runActivity = Activity.Current;
        var activity = StartItemActivity(runActivity, itemId, telemetry?.SpanTags ?? NoTags);
        try
        {
            var scope = new Dictionary<string, object>(telemetry?.LogScope ?? NoScope)
            {
                ["SourceItem"] = itemId,
                ["SourcePort"] = _port
            };
            SweepOutcome outcome;
            using (_logger.BeginScope(scope))
                outcome = await RunInScopeAsync(itemId, createItem, handle, complete, activity, ct);

            RecordOutcome(activity, outcome);
            HostingMetrics.RecordSweptItem(_context.ComponentName, _port, outcome);
            Count(outcome);
            return outcome;
        }
        finally
        {
            activity?.Dispose();
            Activity.Current = runActivity;
        }
    }

    /// <summary>Ends the run: returns <see cref="Summary"/>, or fails a run whose only failure was
    /// a scope cleanup cancelled without a host cancellation.</summary>
    /// <exception cref="InvalidOperationException">An item's scope cleanup was cancelled without a
    /// host cancellation, and no item failed. The recorded outcomes are part of the error.</exception>
    public RunSummary Finish()
    {
        var recorded = Summary;
        if (_unexpectedCleanupCancellations == 0 || recorded.Failed > 0)
            return recorded;

        throw new InvalidOperationException(
            $"Component '{_context.ComponentName}': {_unexpectedCleanupCancellations} scope cleanup(s) failed with an unexpected cancellation " +
            $"the host did not request. The recorded outcome stands: {recorded.Processed} processed, " +
            $"{recorded.Failed} failed, {recorded.Skipped} skipped — but a run with unexpected cancellations is not a clean run.");
    }

    private void Count(SweepOutcome outcome)
    {
        switch (outcome)
        {
            case SweepOutcome.Consumed:
                _processed++;
                break;
            case SweepOutcome.Duplicate:
                _skipped++;
                break;
            case SweepOutcome.Aborted:
                break;
            default:
                _failed++;
                break;
        }
    }

    /// <summary>Starts the root span of the item's own trace, linked to the run's span.</summary>
    private Activity? StartItemActivity(Activity? runActivity, string itemId,
        IReadOnlyDictionary<string, object?> tags)
    {
        Activity.Current = null;
        var activity = ActivitySourceProvider.ActivitySource.StartActivity(
            $"process {_port}",
            ActivityKind.Internal,
            parentContext: default,
            links: runActivity is null ? null : [new ActivityLink(runActivity.Context)]);
        foreach (var (key, value) in tags)
            activity?.SetTag(key, value);
        // The framework's identity tags win over a source's, as in the logging scope.
        activity?.SetTag("intropy.source.port", _port);
        activity?.SetTag("intropy.source.item", itemId);
        return activity;
    }

    /// <summary>Tags the item's outcome. An item left in its source is an error of its trace; a
    /// more specific status already set while processing it is kept.</summary>
    private static void RecordOutcome(Activity? activity, SweepOutcome outcome)
    {
        if (activity is null)
            return;

        activity.SetTag("intropy.sweep.outcome", HostingMetrics.OutcomeName(outcome));
        if (outcome is SweepOutcome.Failed && activity.Status == ActivityStatusCode.Unset)
            activity.SetStatus(ActivityStatusCode.Error, "The item was not handled; it stays in its source for the next run");
    }

    private async Task<SweepOutcome> RunInScopeAsync<TItem>(
        string itemId,
        Func<IServiceProvider, TItem> createItem,
        Func<TItem, CancellationToken, Task<SweepOutcome>> handle,
        Func<TItem, SweepOutcome, Task> complete,
        Activity? activity,
        CancellationToken ct) where TItem : SweptItem
    {
        AsyncServiceScope? scope = null;
        try
        {
            TItem item;
            SweepOutcome outcome;
            try
            {
                scope = _context.Services.CreateAsyncScope();
                item = createItem(scope.Value.ServiceProvider);
                outcome = await handle(item, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return SweepOutcome.Aborted;
            }
            catch (OperationCanceledException e)
            {
                _logger.LogWarning(e, "Processing {SourceItem} was unexpectedly cancelled without a host cancellation; leaving it for the next run",
                    itemId);
                RecordFailure(activity, e);
                return SweepOutcome.Failed;
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "Processing {SourceItem} failed unexpectedly; leaving it for the next run", itemId);
                RecordFailure(activity, e);
                return SweepOutcome.Failed;
            }

            switch (outcome)
            {
                case SweepOutcome.Consumed or SweepOutcome.Duplicate:
                    return await CompleteAsync(item, outcome, complete, activity);
                case SweepOutcome.Aborted when ct.IsCancellationRequested:
                    return SweepOutcome.Aborted;
                case SweepOutcome.Aborted:
                    _logger.LogWarning("Processing {SourceItem} was aborted without a host cancellation; leaving it for the next run",
                        itemId);
                    return SweepOutcome.Failed;
                default:
                    return SweepOutcome.Failed;
            }
        }
        finally
        {
            await DisposeScopeQuietlyAsync(scope, itemId, ct);
        }
    }

    /// <returns><paramref name="outcome"/> once the item is completed;
    /// <see cref="SweepOutcome.Failed"/> when completion failed and the item stays. The handler
    /// already published, so the next run handles the item again; idempotency, where the
    /// component has it, cancels the duplicate.</returns>
    private async Task<SweepOutcome> CompleteAsync<TItem>(TItem item, SweepOutcome outcome,
        Func<TItem, SweepOutcome, Task> complete, Activity? activity) where TItem : SweptItem
    {
        try
        {
            // Completion finishes an already started item and is always awaited.
            await complete(item, outcome);
            return outcome;
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Could not complete {SourceItem} after processing; the next run handles it again",
                item.Id);
            RecordFailure(activity, e);
            return SweepOutcome.Failed;
        }
    }

    private static void RecordFailure(Activity? activity, Exception e)
    {
        activity?.AddException(e);
        activity?.SetStatus(ActivityStatusCode.Error, e.Message);
    }

    /// <summary>Disposes an item scope without letting a cleanup failure escape: the item's
    /// outcome is already recorded. Unexpected cancellations are counted so an otherwise clean run
    /// is still escalated (<see cref="Finish"/>).</summary>
    private async ValueTask DisposeScopeQuietlyAsync(AsyncServiceScope? scope, string itemId, CancellationToken ct)
    {
        if (scope is null)
            return;

        try
        {
            await scope.Value.DisposeAsync();
        }
        catch (OperationCanceledException e) when (!ct.IsCancellationRequested)
        {
            _unexpectedCleanupCancellations++;
            _logger.LogWarning(e, "Cleaning up the processing scope failed with an unexpected cancellation for {SourceItem}; the recorded outcomes stand",
                itemId);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Cleaning up the processing scope was interrupted by the host cancellation for {SourceItem}; the recorded outcomes stand",
                itemId);
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Cleaning up the processing scope failed for {SourceItem}; the recorded outcomes stand", itemId);
        }
    }
}

/// <summary>What a source adds to the telemetry of each of its items, next to what
/// <see cref="SweepItemRunner"/> records for every item (the port, the item and its outcome):
/// for a file, its name.</summary>
public sealed record SweepItemTelemetry
{
    /// <summary>Tags added to the item's <c>process {port}</c> span, such as <c>file.name</c>. The
    /// framework's <c>intropy.source.port</c>, <c>intropy.source.item</c> and
    /// <c>intropy.sweep.outcome</c> always take precedence.</summary>
    public IReadOnlyDictionary<string, object?> SpanTags { get; init; } = new Dictionary<string, object?>();

    /// <summary>Entries added to the logging scope of everything logged while the item is
    /// processed, such as <c>FileName</c>. The framework's <c>SourceItem</c> and <c>SourcePort</c>
    /// always take precedence.</summary>
    public IReadOnlyDictionary<string, object> LogScope { get; init; } = new Dictionary<string, object>();
}
