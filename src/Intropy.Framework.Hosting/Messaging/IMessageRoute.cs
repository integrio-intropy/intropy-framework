using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// One route of a component that consumes messages: which messages it handles, and the pipeline it
/// runs them through, with its generic types closed over. A loader's routes match on the CloudEvent
/// type; a Transactional Integration's send routes match on what the framework recorded about the
/// swept file.
/// </summary>
internal interface IMessageRoute
{
    /// <summary>The route's name in logs, spans and metrics; null for a route that reports
    /// none.</summary>
    string? Name { get; }

    /// <summary>Whether the route handles every message: the single route of a component that does
    /// not route.</summary>
    bool IsCatchAll { get; }

    /// <summary>Whether the route handles <paramref name="message"/>.</summary>
    bool Matches(IncomingMessage message);

    /// <summary>Builds the route's pipeline once, so a missing registration fails at startup.</summary>
    void Verify(IServiceProvider scope, string componentName);

    /// <summary>Runs <paramref name="message"/> through the route, in its own DI scope.</summary>
    /// <param name="message">The consumed message.</param>
    /// <param name="scopes">Creates the DI scope the pipeline is built in.</param>
    /// <param name="componentName">The component, for the pipeline name and errors.</param>
    /// <param name="interrupt">Cancelled when the host interrupts in-flight work; distinguishes an
    /// interruption from a failed or timed-out pipeline.</param>
    /// <param name="cancellationToken">Cancels the pipeline.</param>
    /// <returns>The message's outcome.</returns>
    Task<PipelineOutcome> ExecuteAsync(IncomingMessage message, IServiceScopeFactory scopes, string componentName,
        CancellationToken interrupt, CancellationToken cancellationToken);
}

internal static class RouteComposition
{
    /// <summary>Builds a route's pipeline, naming the block kind, the component and the route in a
    /// composition error.</summary>
    internal static T Build<T>(string blockKind, string? routeName, string componentName, Func<T> build)
    {
        try
        {
            return build();
        }
        catch (InvalidOperationException error)
        {
            var route = routeName is null ? "" : $" route '{routeName}'";
            throw new InvalidOperationException($"{blockKind} '{componentName}'{route} composition failed: {error.Message}",
                error);
        }
    }
}
