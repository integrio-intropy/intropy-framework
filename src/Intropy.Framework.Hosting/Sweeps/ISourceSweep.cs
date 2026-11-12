using Intropy.Framework.Hosting.Jobs;

namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// One run's sweep of one source port, independent of the component that runs it: lists the
/// port's items, hands each to a handler inside its own scope, and completes it in the source
/// only after the handler reports it <see cref="SweepOutcome.Consumed"/> or a
/// <see cref="SweepOutcome.Duplicate"/>. What an item is and what completing it means are the
/// source's; what a handler does with an item is the component's.
/// </summary>
/// <remarks>
/// <para>
/// Derive from <see cref="SourceSweep{TEntry, TItem}"/>: it follows every rule a framework sweep
/// follows, and a kind of source supplies only its listing, its items, their completion and its
/// validation.
/// </para>
/// <para>
/// Implement this interface directly only for a source that does not fit "list, then handle each
/// entry". Handle each item with a <see cref="SweepItemRunner"/>, so it gets the same scope, trace,
/// logging, metrics, cancellation handling and completion rules as files; between items, check the
/// host's cancellation and stop after an <see cref="SweepOutcome.Aborted"/> item; let a listing
/// failure throw, never report it as an empty successful sweep; and end with
/// <see cref="SweepItemRunner.Finish"/>.
/// </para>
/// </remarks>
public interface ISourceSweep
{
    /// <summary>The port this sweep reads.</summary>
    string Port { get; }

    /// <summary>Checks, before anything is read from any of the component's ports, that this port
    /// can be swept (for example, that its adapter is registered).</summary>
    /// <param name="ct">The host's cancellation token.</param>
    /// <exception cref="InvalidOperationException">The port cannot be swept.</exception>
    Task ValidateAsync(CancellationToken ct);

    /// <summary>Sweeps the port once.</summary>
    /// <param name="handle">Handles one item and reports its outcome. It must not complete the item
    /// itself: the sweep does, after it returns.</param>
    /// <param name="ct">The host's cancellation token.</param>
    /// <returns>The port's handled (<see cref="RunSummary.Processed"/>), duplicate
    /// (<see cref="RunSummary.Skipped"/>) and failed items. A cancelled sweep returns the counts
    /// recorded so far.</returns>
    Task<RunSummary> SweepAsync(Func<SweptItem, CancellationToken, Task<SweepOutcome>> handle, CancellationToken ct);
}
