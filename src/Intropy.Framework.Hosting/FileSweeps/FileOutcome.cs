namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// What processing one swept file amounted to, as reported by the file handler.
/// </summary>
public enum FileOutcome
{
    /// <summary>Fully handled — published, or routed as a delivered business incident. The
    /// sweep completes the source (see <see cref="FileCompletion"/>) and counts it as processed.</summary>
    Consumed,

    /// <summary>A duplicate of an already handled item. The sweep completes the source and
    /// counts it as skipped.</summary>
    Duplicate,

    /// <summary>Not handled. The source stays for the next run and counts as failed.</summary>
    Failed,

    /// <summary>Interrupted. With a host cancellation the source stays, is not counted, and the
    /// sweep stops; without one it is a failure of the file.</summary>
    Aborted,
}
