using System.Diagnostics;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// The Transactional Integration's run-to-completion job: in one run it sweeps the source,
/// publishing each file to the integration's queue through the receive pipeline, while the
/// subscriber consumes the queue through the send pipeline. Hosted by the <see cref="JobRunner"/>,
/// which owns the process around it: the sidecar lifecycle, job tracing, and exit-code mapping.
/// </summary>
/// <typeparam name="TCtx">The integration's context type, shared by the receive and send pipelines.</typeparam>
public class TransactionalIntegrationJob<TCtx> : IJob where TCtx : Context
{
    private readonly TransactionalIntegrationReceiver<TCtx> _receiver;
    private readonly MessageSubscriber<TCtx> _messageSubscriber;
    private readonly ILogger<TransactionalIntegrationJob<TCtx>> _logger;

    /// <summary>
    /// Creates a new instance of <see cref="TransactionalIntegrationJob{TCtx}"/>.
    /// </summary>
    /// <param name="receiver">The receive side: sweeps the integration's source through the
    /// receive pipeline, completing a file (delete or archive) only after it is on the queue.</param>
    /// <param name="messageSubscriber">The send side: consumes the integration's queue through the
    /// send pipeline, until the subscription goes idle.</param>
    /// <param name="loggerFactory">An instance of <see cref="ILoggerFactory"/>.</param>
    internal TransactionalIntegrationJob(
        TransactionalIntegrationReceiver<TCtx> receiver,
        MessageSubscriber<TCtx> messageSubscriber,
        ILoggerFactory loggerFactory)
    {
        _receiver = receiver;
        _messageSubscriber = messageSubscriber;
        _logger = loggerFactory.CreateLogger<TransactionalIntegrationJob<TCtx>>();
    }

    /// <summary>
    /// Runs the job once: sweeps the source while subscribing, and
    /// returns once publishing is done and the subscription has gone idle (or the host cancelled).
    /// </summary>
    /// <returns>Files published (and completed) as <c>Processed</c> and duplicates as
    /// <c>Skipped</c>. <c>Failed</c> counts both sides: files left in place, and messages the run
    /// left for redelivery — so a run whose deliveries failed exits 1 like one whose files did. The
    /// consumed messages' own counts are tagged on the job's span
    /// (<c>intropy.messages.processed</c>, <c>.failed</c>, <c>.skipped</c>).</returns>
    public async Task<RunSummary> ExecuteAsync(CancellationToken ct)
    {
        var coordinator = new LifecycleCoordinator();

        var publisherTask = Task.Run(async () =>
        {
            try
            {
                _logger.LogInformation("Starting source item processing");
                var summary = await _receiver.SweepAsync(ct);
                _logger.LogInformation("Source item processing completed");
                coordinator.SignalPublishingComplete();
                return summary;
            }
            catch (Exception ex)
            {
                coordinator.SignalPublishingFailed(ex);
                throw;
            }
        });

        var subscriberTask = _messageSubscriber.ExecuteAsync(coordinator.PublishingCompleteSignal, ct);

        await Task.WhenAll(publisherTask, subscriberTask);
        var files = await publisherTask;
        var messages = await subscriberTask;

        // The current span is the job's (JobRunner).
        var job = Activity.Current;
        job?.SetTag("intropy.messages.processed", messages.Processed);
        job?.SetTag("intropy.messages.failed", messages.Failed);
        job?.SetTag("intropy.messages.skipped", messages.Skipped);

        return files with { Failed = files.Failed + messages.Failed };
    }
}
