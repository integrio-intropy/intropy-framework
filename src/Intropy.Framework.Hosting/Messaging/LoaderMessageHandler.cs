using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// Handles consumed messages, whatever transport delivered them: finds each message's route, runs
/// the route (a message route one message at a time, a batch route the batch's messages at once)
/// and reports every message's outcome. Owns nothing about acknowledgement — the transport maps the
/// outcome to its own ack.
/// </summary>
internal sealed class LoaderMessageHandler(
    IServiceScopeFactory scopes,
    LoaderRouteTable routes,
    string componentName,
    ILogger<LoaderMessageHandler> logger)
{
    /// <summary>Handles one message.</summary>
    /// <param name="message">The consumed message.</param>
    /// <param name="interrupt">Cancelled when the host interrupts in-flight work.</param>
    /// <param name="cancellationToken">Cancels the pipeline (the interrupt or the transport's
    /// per-message timeout).</param>
    internal async Task<LoaderMessageResult> HandleAsync(IncomingMessage message, CancellationToken interrupt,
        CancellationToken cancellationToken) =>
        (await HandleBatchAsync([message], interrupt, cancellationToken))[0];

    /// <summary>Handles a delivered batch: each route gets its messages in delivery order.</summary>
    /// <returns>One result per message, in the order of <paramref name="messages"/>.</returns>
    internal async Task<IReadOnlyList<LoaderMessageResult>> HandleBatchAsync(IReadOnlyList<IncomingMessage> messages,
        CancellationToken interrupt, CancellationToken cancellationToken)
    {
        var results = new LoaderMessageResult[messages.Count];
        var byRoute = new Dictionary<ILoaderRoute, List<int>>();
        for (var i = 0; i < messages.Count; i++)
        {
            var route = routes.Find(messages[i].CloudEvent.Type);
            if (route is null)
            {
                logger.LogWarning("No route handles message {MessageId} of event type {EventType}",
                    messages[i].MessageId, messages[i].CloudEvent.Type);
                results[i] = new LoaderMessageResult(null, new PipelineOutcome(MessageOutcome.Unrouted, "unrouted",
                    $"No route handles event type '{messages[i].CloudEvent.Type}'"));
                continue;
            }

            if (!byRoute.TryGetValue(route, out var indexes))
                byRoute[route] = indexes = [];
            indexes.Add(i);
        }

        foreach (var (route, indexes) in byRoute)
        {
            var outcomes = await route.ExecuteAsync([.. indexes.Select(i => messages[i])], scopes, componentName,
                interrupt, cancellationToken);
            for (var j = 0; j < indexes.Count; j++)
            {
                var message = messages[indexes[j]];
                Log(route, message, outcomes[j]);
                results[indexes[j]] = new LoaderMessageResult(route, outcomes[j]);
            }
        }

        return results;
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
            case MessageOutcome.Filtered:
                logger.LogDebug("Message {MessageId} on route {Route} was filtered out; consumed", message.MessageId,
                    route.Name);
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
