using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// The <see cref="MessageHandler"/> of a component that routes its messages (a loader, a
/// Transactional Integration's send side): finds the message's route, runs it and reports the
/// message's outcome, or <see cref="MessageOutcome.Unrouted"/> when no route handles it. Owns
/// nothing about acknowledgement, spans, metrics or outcome logs — the <see cref="MessageConsumer"/> does.
/// </summary>
/// <param name="scopes">Creates the DI scope each route's pipeline is built in.</param>
/// <param name="routes">The component's routes.</param>
/// <param name="componentName">The component, for pipeline names and errors.</param>
/// <param name="describeUnrouted">Describes why no route handles a message, for its outcome.</param>
/// <param name="defaultPathIsUnrouted">Whether a message the subscription delivered on its default
/// route is unrouted whatever it carries: true when the subscription's rules (a content filter)
/// select the messages, as a loader's do; false when the subscription has no rules and delivers
/// everything on its default route, as a Transactional Integration's internal hop does.</param>
internal sealed class RoutingMessageHandler(
    IServiceScopeFactory scopes,
    MessageRouteTable routes,
    string componentName,
    Func<IncomingMessage, string> describeUnrouted,
    bool defaultPathIsUnrouted)
{
    internal async Task<HandledMessage> HandleAsync(IncomingMessage message, Activity? activity,
        CancellationToken interrupt, CancellationToken cancellationToken)
    {
        if (defaultPathIsUnrouted && message.IsUnhandled)
            return new HandledMessage(new PipelineOutcome(MessageOutcome.Unrouted, PipelineOutcome.Unrouted,
                $"The subscription's rules select no route for this event of type '{message.Type}'"));

        var route = routes.Find(message);
        if (route is null)
            return new HandledMessage(new PipelineOutcome(MessageOutcome.Unrouted, PipelineOutcome.Unrouted,
                describeUnrouted(message)));

        var outcome = await route.ExecuteAsync(message, scopes, componentName, interrupt, cancellationToken);
        return new HandledMessage(outcome, route.Name);
    }
}
