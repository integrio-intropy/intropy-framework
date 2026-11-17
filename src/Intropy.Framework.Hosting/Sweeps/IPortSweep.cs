using Intropy.Framework.Hosting.Jobs;

namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// The sweep of one source port: what <see cref="SourcesSweep"/> runs once per port a swept
/// component declares. Validation is separate from the sweep, so every port of a component is
/// checked before anything is read from any port.
/// </summary>
internal interface IPortSweep
{
    /// <summary>The port this sweep reads and completes items on.</summary>
    string Port { get; }

    /// <summary>Checks, before anything is read, that the port can be swept: its source is
    /// reachable from the component (for example, its adapter is registered) and its pipeline can
    /// be composed.</summary>
    /// <param name="ct">The host's cancellation token.</param>
    /// <exception cref="InvalidOperationException">The port cannot be swept.</exception>
    Task ValidateAsync(CancellationToken ct);

    /// <summary>Sweeps the port once.</summary>
    /// <param name="ct">The host's cancellation token.</param>
    /// <returns>The port's handled, duplicate and failed items. A cancelled sweep returns the
    /// counts recorded so far.</returns>
    Task<RunSummary> SweepAsync(CancellationToken ct);
}
