using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// The receive side of a Transactional Integration: one sweep of the source port
/// (<see cref="ReceivePortSweep{TCtx}"/>), where each source file is run through the receive
/// pipeline, which publishes it to the integration's queue. A file is completed (deleted or
/// archived) only after it is on the queue.
/// </summary>
/// <typeparam name="TCtx">The integration's context type.</typeparam>
internal sealed class TransactionalIntegrationReceiver<TCtx>(
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory)
    where TCtx : Context
{
    private readonly SourceSweep _sweep = new(provider, frameworkOptions, source =>
        new ReceivePortSweep<TCtx>(source, provider, frameworkOptions, contextFactory, loggerFactory),
        loggerFactory);

    /// <summary>Sweeps the source once.</summary>
    /// <returns>Files published (and completed) as <c>Processed</c>, duplicates as
    /// <c>Skipped</c>, and files left in place as <c>Failed</c>.</returns>
    /// <exception cref="InvalidOperationException">Thrown before any file is listed when the
    /// integration cannot sweep its source; listing failures propagate too.</exception>
    public Task<RunSummary> SweepAsync(CancellationToken ct) => _sweep.SweepAsync(ct);
}
