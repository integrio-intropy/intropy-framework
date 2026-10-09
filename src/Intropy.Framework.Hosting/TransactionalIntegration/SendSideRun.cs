using System.Collections.Concurrent;
using System.Diagnostics;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// The send side of one run: serves the Dapr gRPC app callback through which the sidecar pushes
/// the integration's queue, hands each message to the <see cref="MessageProcessor{TCtx}"/>, waits
/// for the queue to go idle once publishing is done, and drains before the callback stops. Tallies
/// every message's latest outcome in the run for the run's summary.
/// </summary>
/// <param name="processor">Processes each consumed message through the send pipeline.</param>
/// <param name="options">Configuration options for the integration.</param>
/// <param name="componentName">The component name, for logs, spans and metrics.</param>
/// <param name="loggerFactory">Creates the send side's and the consumer's loggers.</param>
/// <param name="timeProvider">Time source for the idle clock. Defaults to
/// <see cref="TimeProvider.System"/>. Override in tests to control time.</param>
/// <typeparam name="TCtx">The send pipeline's context type.</typeparam>
internal class SendSideRun<TCtx>(
    MessageProcessor<TCtx> processor,
    TransactionalIntegrationOptions options,
    string componentName,
    ILoggerFactory loggerFactory,
    TimeProvider? timeProvider = null) where TCtx : Context
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<SendSideRun<TCtx>>();

    /// <summary>Serves the callback and waits for completion, with nobody told of deliveries.</summary>
    public Task<RunSummary> ExecuteAsync(Task publishingCompleteSignal, CancellationToken ct = default) =>
        ExecuteAsync(publishingCompleteSignal, delivered: null, ct);

    /// <summary>
    /// Serves the callback and waits for completion: until publishing is done and the queue has
    /// gone idle, or the host cancels.
    /// </summary>
    /// <param name="publishingCompleteSignal">A task that completes when all files have been published.</param>
    /// <param name="ct">The host's cancellation: it stops waiting for the publisher and the idle
    /// timeout; messages in flight still get the grace period before the callback stops.</param>
    /// <param name="delivered">Called only for probes, with their message id: lets the
    /// <see cref="InternalQueueReadinessGate"/> correlate delivery with probes from this run.</param>
    /// <returns>The messages this run consumed, by their last outcome: acknowledged as
    /// <c>Processed</c>, idempotent duplicates as <c>Skipped</c>, and messages left for
    /// redelivery — returned for retry, or still in flight when the grace period ended — as
    /// <c>Failed</c>. A message interrupted because the host is stopping is left for redelivery
    /// but not counted: host cancellation is not a failure. A internal queue probe is acknowledged and not
    /// counted either.</returns>
    public async Task<RunSummary> ExecuteAsync(Task publishingCompleteSignal, Action<string>? delivered,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publishingCompleteSignal);
        var outcomes = new ConcurrentDictionary<string, MessageOutcome>();

        // The latest outcome in the run counts; an interruption by the host stopping does not
        // replace an earlier one.
        async Task<HandledMessage> Tally(IncomingMessage message, Activity? activity,
            CancellationToken interrupt, CancellationToken cancellationToken)
        {
            // A probe carries nothing to send. Acknowledge stale probes too, but let the gate
            // decide whether this id proves readiness for the current run.
            if (message.Type == InternalQueueMessageTypes.Probe)
            {
                delivered?.Invoke(message.MessageId);
                return new HandledMessage(new PipelineOutcome(MessageOutcome.Probe));
            }

            var handled = await processor.HandleAsync(message, activity, interrupt, cancellationToken);
            if (handled.Outcome.Outcome is not MessageOutcome.Interrupted)
                outcomes[message.MessageId] = handled.Outcome.Outcome;
            return handled;
        }

        // The run's state is the run's, not the singleton run's: the subscription, its
        // outcome tally and idle clock live and die with ExecuteAsync, so two runs can never see
        // each other.
        await using var subscription = await SubscriptionHost.StartAsync(
            new MessageConsumerSettings(options.DaprPubSubName, options.DaprTopicName,
                options.MaxMessageProcessingTime, options.PostIdleGracePeriod),
            Tally, options.CallbackPort, componentName, loggerFactory,
            run: Activity.Current?.Context ?? default, timeProvider: timeProvider,
            cancellationToken: CancellationToken.None);
        _logger.LogInformation("Serving the Dapr app callback on port {Port} for topic {Topic} on {PubSub}",
            subscription.Port, options.DaprTopicName, options.DaprPubSubName);

        var unfinished = 0;
        try
        {
            _logger.LogInformation("Waiting for publisher to complete...");
            await publishingCompleteSignal.WaitAsync(ct);
            _logger.LogInformation("Publisher completed. Starting idle timeout monitoring.");

            // The idle window measures quiet time after the sweep, not the process's whole
            // lifetime: the clock restarts here so a sweep longer than IdleTimeout cannot make
            // the first poll look already-idle and exit with messages still on the queue.
            subscription.Consumer.InFlight.RestartIdleClock();
            await new IdleTimeoutMonitor(subscription.Consumer.InFlight, options.IdleTimeout, _logger, timeProvider)
                .WaitForIdleTimeoutAsync(ct);

            _logger.LogInformation("Idle timeout triggered. Beginning graceful shutdown.");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _logger.LogInformation("The run was cancelled.");
        }
        finally
        {
            // Drain while the callback still listens, so the acks of messages in flight reach
            // the sidecar; only then does it stop.
            unfinished = await subscription.StopAsync(CancellationToken.None);
        }

        return Summarize(ct.IsCancellationRequested ? 0 : unfinished, outcomes);
    }

    private RunSummary Summarize(int unfinished, ConcurrentDictionary<string, MessageOutcome> outcomes)
    {
        var tallied = outcomes.Values.ToList();
        var summary = new RunSummary(
            Processed: tallied.Count(o => o == MessageOutcome.Processed),
            Failed: tallied.Count(o => o == MessageOutcome.Failed) + unfinished,
            Skipped: tallied.Count(o => o == MessageOutcome.Skipped));
        _logger.LogInformation(
            "Consumed messages for {Component}: {Processed} processed, {Failed} left for redelivery, {Skipped} skipped",
            componentName, summary.Processed, summary.Failed, summary.Skipped);
        return summary;
    }
}
