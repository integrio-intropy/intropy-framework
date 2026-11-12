using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Sweeps;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// The receive side of a Transactional Integration: one sweep of the integration's sources
/// (<see cref="SourcesSweep"/>), one <see cref="ReceivePortSweep{TCtx}"/> per registered
/// <see cref="SourcePort"/>, where each source item is run through the receive pipeline, which
/// publishes it to the integration's queue. An item is completed (deleted or archived) only after
/// it is on the queue.
/// </summary>
/// <typeparam name="TCtx">The integration's context type.</typeparam>
internal sealed class TransactionalIntegrationReceiver<TCtx>(
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory)
    where TCtx : Context
{
    private readonly SourcesSweep _sweep = new(provider, frameworkOptions, source =>
        new ReceivePortSweep<TCtx>(source, provider, frameworkOptions, contextFactory, loggerFactory),
        loggerFactory);

    /// <summary>Sweeps the integration's sources once: one port sweep per registered source port.</summary>
    /// <returns>Items published (and completed) as <c>Processed</c>, duplicates as
    /// <c>Skipped</c>, and items left in place as <c>Failed</c>.</returns>
    /// <exception cref="InvalidOperationException">Thrown before any item is listed when the
    /// integration cannot sweep its source, and after the last port when a port failed: the error
    /// names every failed port, and its inner <see cref="AggregateException"/> carries the ports'
    /// failures.</exception>
    public Task<RunSummary> SweepAsync(CancellationToken ct) => _sweep.SweepAsync(ct);
}
