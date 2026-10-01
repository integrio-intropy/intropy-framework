using System.Diagnostics;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// Handles consumed messages, whatever transport delivered them: finds the message's route, runs
/// it and reports the message's outcome. Owns nothing about acknowledgement — the transport maps the
/// outcome to its own ack.
/// </summary>
internal sealed class LoaderMessageHandler(
    IServiceScopeFactory scopes,
    LoaderRouteTable routes,
    string componentName,
    ILogger<LoaderMessageHandler> logger)
{
    /// <summary>Handles one message in its own consumer span and records it in the consumed-messages
    /// metrics — what every transport does with a delivered message before acknowledging it.</summary>
    /// <param name="message">The consumed message.</param>
    /// <param name="extensions">The CloudEvent extensions it was delivered with (trace context,
    /// <c>retrycount</c>).</param>
    /// <param name="topic">The topic it was consumed from.</param>
    /// <param name="interrupt">Cancelled when the host interrupts in-flight work.</param>
    /// <param name="cancellationToken">Cancels the pipeline (the interrupt or the per-message
    /// timeout).</param>
    internal async Task<PipelineOutcome> ProcessAsync(IncomingMessage message,
        IReadOnlyDictionary<string, Value> extensions, string topic, CancellationToken interrupt,
        CancellationToken cancellationToken)
    {
        var start = Stopwatch.GetTimestamp();
        using var activity = DaprActivityHelper.StartProcessActivity(message.MessageId, extensions, topic, default);
        activity?.SetTag("cloudevents.event_type", message.CloudEvent.Type);

        var (route, outcome) = await HandleAsync(message, interrupt, cancellationToken);

        if (route is not null)
            activity?.SetTag("intropy.route", route.Name);
        if (outcome.ErrorType is not null)
        {
            activity?.SetTag("error.type", outcome.ErrorType);
            activity?.SetStatus(ActivityStatusCode.Error, outcome.Description);
        }

        HostingMetrics.RecordProcessedMessage(componentName, topic, HostingMetrics.OutcomeName(outcome.Outcome),
            outcome.ErrorType, Stopwatch.GetElapsedTime(start), route?.Name);
        return outcome;
    }

    /// <summary>Handles one message.</summary>
    /// <param name="message">The consumed message.</param>
    /// <param name="interrupt">Cancelled when the host interrupts in-flight work.</param>
    /// <param name="cancellationToken">Cancels the pipeline (the interrupt or the transport's
    /// per-message timeout).</param>
    internal async Task<LoaderMessageResult> HandleAsync(IncomingMessage message, CancellationToken interrupt,
        CancellationToken cancellationToken)
    {
        var route = routes.Find(message.CloudEvent.Type);
        if (route is null)
        {
            logger.LogWarning("No route handles message {MessageId} of event type {EventType}",
                message.MessageId, message.CloudEvent.Type);
            return new LoaderMessageResult(null, new PipelineOutcome(MessageOutcome.Unrouted, "unrouted",
                $"No route handles event type '{message.CloudEvent.Type}'"));
        }

        var outcome = await route.ExecuteAsync(message, scopes, componentName, interrupt, cancellationToken);
        Log(route, message, outcome);
        return new LoaderMessageResult(route, outcome);
    }

    private void Log(ILoaderRoute route, IncomingMessage message, PipelineOutcome outcome)
    {
        switch (outcome.Outcome)
        {
            case MessageOutcome.Processed:
                logger.LogInformation("Loaded message {MessageId} on route {Route}", message.MessageId, route.Name);
                break;
            case MessageOutcome.Skipped:
                logger.LogInformation("Message {MessageId} on route {Route} is a duplicate; consumed",
                    message.MessageId, route.Name);
                break;
            case MessageOutcome.Interrupted:
                logger.LogInformation("Message {MessageId} was interrupted by shutdown; left for redelivery",
                    message.MessageId);
                break;
            default:
                logger.LogWarning("Processing message {MessageId} on route {Route} failed ({ErrorType}: {Description}); left for redelivery",
                    message.MessageId, route.Name, outcome.ErrorType, outcome.Description);
                break;
        }
    }
}

/// <summary>How one handled message ended, and the route that handled it (null when unrouted).</summary>
internal readonly record struct LoaderMessageResult(ILoaderRoute? Route, PipelineOutcome Outcome);
