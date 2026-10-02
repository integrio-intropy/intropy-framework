using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// A loader's <see cref="MessageHandler"/>: finds the route for the message's CloudEvent type, runs
/// it and reports the message's outcome. A message the subscription delivered on its default route
/// is unrouted whatever its type: the subscription's rules (a content filter) left it out. Owns
/// nothing about acknowledgement, spans, metrics or outcome logs — the <see cref="MessageConsumer"/> does.
/// </summary>
internal sealed class LoaderMessageHandler(
    IServiceScopeFactory scopes,
    LoaderRouteTable routes,
    string componentName)
{
    internal async Task<HandledMessage> HandleAsync(IncomingMessage message, Activity? activity,
        CancellationToken interrupt, CancellationToken cancellationToken)
    {
        if (message.IsUnhandled)
            return new HandledMessage(new PipelineOutcome(MessageOutcome.Unrouted, PipelineOutcome.Unrouted,
                $"The subscription's rules select no route for this event of type '{message.Type}'"));

        var route = routes.Find(message.Type);
        if (route is null)
            return new HandledMessage(new PipelineOutcome(MessageOutcome.Unrouted, PipelineOutcome.Unrouted,
                $"No route handles event type '{message.Type}'"));

        var outcome = await route.ExecuteAsync(message, message.ToCloudEvent(), scopes, componentName, interrupt,
            cancellationToken);
        return new HandledMessage(outcome, route.Name);
    }
}
