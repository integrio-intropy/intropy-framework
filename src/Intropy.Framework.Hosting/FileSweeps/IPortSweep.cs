using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.Jobs;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// The sweep of one source port: what <see cref="SourceSweep"/> runs once per port a file-driven
/// component declares. Validation is separate from the sweep, so every port of a component is
/// checked before any port's files are listed.
/// </summary>
internal interface IPortSweep
{
    /// <summary>The port this sweep lists and completes files on.</summary>
    string Port { get; }

    /// <summary>Checks, before any file is listed, that the port can be swept: its
    /// <see cref="IFileAdapter"/> is registered and its pipeline can be composed.</summary>
    /// <exception cref="InvalidOperationException">The port cannot be swept.</exception>
    Task ValidateAsync();

    /// <summary>Sweeps the port once.</summary>
    /// <param name="ct">The host's cancellation token.</param>
    /// <returns>The port's handled, duplicate and failed files. A cancelled sweep returns the
    /// counts recorded so far.</returns>
    Task<RunSummary> SweepAsync(CancellationToken ct);
}
