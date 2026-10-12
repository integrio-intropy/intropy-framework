using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// One run's sweep of a file-driven component's sources: one <see cref="IPortSweep"/> per
/// registered <see cref="SourcePort"/>, validated before any file is listed, then swept one port
/// after another in registration order. It is a part of a run, not the job itself: the
/// component's <see cref="IJob"/> runs it.
/// </summary>
/// <remarks>
/// The ports are resolved when the sweep runs, not when it is built, so a component without a
/// source port fails its run (and the runner maps that to an exit code) instead of failing to
/// start.
/// </remarks>
/// <param name="provider">The root provider the source ports are resolved from.</param>
/// <param name="frameworkOptions">The component's identity, for errors.</param>
/// <param name="createSweep">Creates the sweep of one port.</param>
internal sealed class SourceSweep(
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    Func<SourcePort, IPortSweep> createSweep)
{
    /// <summary>Sweeps every source port once.</summary>
    /// <returns>The ports' summaries added together.</returns>
    /// <exception cref="InvalidOperationException">Thrown before any file is listed when no
    /// source port is registered (<c>AddSourcePort</c>) or a port cannot be swept. A port's
    /// listing failure propagates too; a listing failure is never reported as an empty
    /// successful sweep.</exception>
    public async Task<RunSummary> SweepAsync(CancellationToken ct)
    {
        var ports = provider.GetServices<SourcePort>().ToList();
        if (ports.Count == 0)
            throw new InvalidOperationException(
                $"Component '{frameworkOptions.ComponentName}': no source port is registered. Call AddSourcePort.");

        var sweeps = ports.Select(createSweep).ToList();
        foreach (var sweep in sweeps)
            await sweep.ValidateAsync();

        var summary = RunSummary.Empty;
        foreach (var sweep in sweeps)
        {
            var port = await sweep.SweepAsync(ct);
            summary = new RunSummary(summary.Processed + port.Processed, summary.Failed + port.Failed,
                summary.Skipped + port.Skipped);
        }

        return summary;
    }
}
