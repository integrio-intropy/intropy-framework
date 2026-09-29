using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// Handles one consumed message, whatever transport delivered it: finds its route, runs the
/// route's pipeline in the message's own DI scope, and reports the outcome. Owns nothing about
/// acknowledgement — the transport maps the outcome to its own ack.
/// </summary>
internal sealed class LoaderMessageHandler(
    IServiceScopeFactory scopes,
    LoaderRouteTable routes,
    string componentName,
    ILogger<LoaderMessageHandler> logger)
{
    /// <param name="message">The consumed message.</param>
    /// <param name="interrupt">Cancelled when the host interrupts in-flight work.</param>
    /// <param name="cancellationToken">Cancels the pipeline (the interrupt or the transport's
    /// per-message timeout).</param>
    internal async Task<LoaderMessageResult> HandleAsync(IncomingMessage message, CancellationToken interrupt,
        CancellationToken cancellationToken)
    {
        var eventType = message.CloudEvent.Type;
        var route = routes.Find(eventType);
        if (route is null)
        {
            logger.LogWarning("No route handles message {MessageId} of event type {EventType}", message.MessageId,
                eventType);
            return new LoaderMessageResult(null, new PipelineOutcome(MessageOutcome.Unrouted, "unrouted",
                $"No route handles event type '{eventType}'"));
        }

        var outcome = await ExecuteAsync(route, message, interrupt, cancellationToken);
        Log(route, message, outcome);
        return new LoaderMessageResult(route, outcome);
    }

    private async Task<PipelineOutcome> ExecuteAsync(ILoaderRoute route, IncomingMessage message,
        CancellationToken interrupt, CancellationToken cancellationToken)
    {
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            return await route.ExecuteAsync(message, scope.ServiceProvider, componentName, interrupt,
                cancellationToken);
        }
        catch (OperationCanceledException) when (interrupt.IsCancellationRequested)
        {
            return new PipelineOutcome(MessageOutcome.Interrupted);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return PipelineOutcome.Failed("aborted", "Processing exceeded its time limit");
        }
        catch (Exception e)
        {
            logger.LogError(e, "Error processing message {MessageId} on route {Route}", message.MessageId, route.Name);
            return PipelineOutcome.Failed(e.GetType().FullName!, e.Message);
        }
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
