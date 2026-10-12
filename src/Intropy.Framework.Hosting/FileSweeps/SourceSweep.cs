using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// One run's sweep of a file-driven component's sources: one <see cref="IPortSweep"/> per
/// registered <see cref="SourcePort"/>, validated before any file is listed, then swept one port
/// after another in registration order. It is a part of a run, not the job itself: the
/// component's <see cref="IJob"/> runs it.
/// </summary>
/// <remarks>
/// <para>
/// The ports are resolved when the sweep runs, not when it is built, so a component without a
/// source port fails its run (and the runner maps that to an exit code) instead of failing to
/// start.
/// </para>
/// <para>
/// With several ports, a port whose sweep throws does not stop the others: the failure is logged
/// with the port, the remaining ports are swept, and the run then fails naming every failed port.
/// With one port its exception propagates unchanged, and an exception thrown while the host is
/// cancelling always does.
/// </para>
/// </remarks>
/// <param name="provider">The root provider the source ports are resolved from.</param>
/// <param name="frameworkOptions">The component's identity, for errors.</param>
/// <param name="createSweep">Creates the sweep of one port.</param>
/// <param name="loggerFactory">The logger factory.</param>
internal sealed class SourceSweep(
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    Func<SourcePort, IPortSweep> createSweep,
    ILoggerFactory loggerFactory)
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<SourceSweep>();

    /// <summary>Sweeps every source port once.</summary>
    /// <returns>The ports' summaries added together.</returns>
    /// <exception cref="InvalidOperationException">Thrown before any file is listed when no
    /// source port is registered (<c>AddSourcePort</c>) or a port cannot be swept, and after the
    /// last port when one of several ports failed. A single port's failure propagates unchanged; a
    /// listing failure is never reported as an empty successful sweep.</exception>
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
        var failures = new List<(string Port, Exception Error)>();
        foreach (var sweep in sweeps)
        {
            RunSummary port;
            try
            {
                port = await sweep.SweepAsync(ct);
            }
            // Isolating any failure is safe: the run still ends in an exception, so it exits as a
            // failure; only the failed port's counts are lost, and a failed run doesn't report them.
            catch (Exception e) when (sweeps.Count > 1 && !ct.IsCancellationRequested)
            {
                _logger.LogError(e, "Sweeping source port {Port} of {Component} failed; continuing with the other ports",
                    sweep.Port, frameworkOptions.ComponentName);
                failures.Add((sweep.Port, e));
                continue;
            }

            summary = new RunSummary(summary.Processed + port.Processed, summary.Failed + port.Failed,
                summary.Skipped + port.Skipped);
        }

        if (failures.Count > 0)
            throw new InvalidOperationException(
                $"Component '{frameworkOptions.ComponentName}': sweeping source port(s) " +
                $"{string.Join(", ", failures.Select(f => $"'{f.Port}'"))} failed",
                new AggregateException(failures.Select(f => f.Error)));

        return summary;
    }
}
