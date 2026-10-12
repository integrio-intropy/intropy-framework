using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>
/// The extractor's run-to-completion job: one sweep of the source port
/// (<see cref="ExtractorPortSweep{TInput,TOutput,TCtx}"/>), where each source file is read as
/// UTF-8 text and run through the composed <see cref="Extractor{TInput,TOutput,TCtx}"/> pipeline.
/// A published file (or one routed as a delivered business incident) and an idempotent duplicate
/// are completed as the source port says (<c>AddSourcePort</c>); a failed file stays for the next
/// scheduled run — the idempotency check makes the retry safe.
/// </summary>
/// <remarks>
/// Delivery semantics: publication happens before the idempotency recording, and delivery is
/// at-least-once, not exactly-once. A crash after publication but before the record means the
/// next run processes the file again and may publish it a second time. (A source file that could
/// not be deleted after a fully recorded success is different: the record exists, so the
/// redelivery is cancelled as a duplicate.)
/// </remarks>
internal sealed class ExtractorJob<TInput, TOutput, TCtx>(
    IServiceProvider provider,
    FrameworkOptions frameworkOptions,
    ContextFactory<TCtx> contextFactory,
    ILoggerFactory loggerFactory) : IJob
    where TCtx : Context
{
    private readonly SourceSweep _sweep = new(provider, frameworkOptions, source =>
        new ExtractorPortSweep<TInput, TOutput, TCtx>(source, provider, frameworkOptions, contextFactory, loggerFactory));

    /// <summary>Runs one sweep of the source port.</summary>
    public Task<RunSummary> ExecuteAsync(CancellationToken ct) => _sweep.SweepAsync(ct);
}
