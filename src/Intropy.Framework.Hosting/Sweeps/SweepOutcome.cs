namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// What processing one swept item amounted to, as reported by the item handler. The source decides
/// what completing an item means: a file is deleted or archived
/// (<see cref="FileSweeps.FileCompletion"/>).
/// </summary>
public enum SweepOutcome
{
    /// <summary>Fully handled — published, or routed as a delivered business incident. The
    /// sweep completes the item in its source and counts it as processed.</summary>
    Consumed,

    /// <summary>A duplicate of an already handled item. The sweep completes the item in its source
    /// and counts it as skipped.</summary>
    Duplicate,

    /// <summary>Not handled. The item stays in its source for the next run and counts as failed.</summary>
    Failed,

    /// <summary>Interrupted. With a host cancellation the item stays, is not counted, and the
    /// sweep stops; without one it is a failure of the item.</summary>
    Aborted,
}
