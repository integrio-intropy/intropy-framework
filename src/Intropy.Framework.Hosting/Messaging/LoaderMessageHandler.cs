using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// A loader's <see cref="MessageHandler"/>: finds the route for the message's CloudEvent type, runs
/// it and reports the message's outcome. A message the subscription delivered on its default route
/// is unrouted whatever its type: the subscription's rules (a content filter) left it out. Owns
/// nothing about acknowledgement, spans or metrics — the <see cref="MessageConsumer"/> does.
/// </summary>
internal sealed class LoaderMessageHandler(
    IServiceScopeFactory scopes,
    LoaderRouteTable routes,
    string componentName,
    ILogger<LoaderMessageHandler> logger)
{
    internal async Task<HandledMessage> HandleAsync(IncomingMessage message, Activity? activity,
        CancellationToken interrupt, CancellationToken cancellationToken)
    {
        if (message.IsUnhandled)
        {
            logger.LogWarning("The subscription's rules select no route for message {MessageId} of event type {EventType}",
                message.MessageId, message.Type);
            return new HandledMessage(new PipelineOutcome(MessageOutcome.Unrouted, "unrouted",
                $"The subscription's rules select no route for this event of type '{message.Type}'"));
        }

        var route = routes.Find(message.Type);
        if (route is null)
        {
            logger.LogWarning("No route handles message {MessageId} of event type {EventType}",
                message.MessageId, message.Type);
            return new HandledMessage(new PipelineOutcome(MessageOutcome.Unrouted, "unrouted",
                $"No route handles event type '{message.Type}'"));
        }

        var outcome = await route.ExecuteAsync(message, message.ToCloudEvent(), scopes, componentName, interrupt,
            cancellationToken);
        Log(route, message, outcome);
        return new HandledMessage(outcome, route.Name);
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
