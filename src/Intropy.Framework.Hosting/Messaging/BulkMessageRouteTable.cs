using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// Finds the route for a bulk delivery: the first route, in declaration order, that matches it. A
/// component either has one route for every message or matching routes, never both. Built once, at
/// registration.
/// </summary>
/// <remarks>
/// A batch is routed as a whole, not entry by entry. The sidecar evaluates the subscription's
/// routing rules per message and groups the matches before it delivers, so every entry of one
/// delivery took the same route — the consumer asserts that rather than re-deriving it.
/// </remarks>
internal sealed class BulkMessageRouteTable
{
    private readonly IBulkMessageRoute? _catchAll;

    internal BulkMessageRouteTable(IReadOnlyList<IBulkMessageRoute> routes)
    {
        if (routes.Count == 0)
            throw new ArgumentException("A component that consumes messages needs at least one route.", nameof(routes));

        _catchAll = routes.FirstOrDefault(r => r.IsCatchAll);
        if (_catchAll is not null && routes.Count > 1)
            throw new ArgumentException("A route for every message cannot be combined with other routes.",
                nameof(routes));

        Routes = routes;
    }

    /// <summary>Every route, in declaration order.</summary>
    internal IReadOnlyList<IBulkMessageRoute> Routes { get; }

    /// <summary>Whether the component routes (and so can receive unrouted messages).</summary>
    internal bool IsRouting => _catchAll is null;

    /// <summary>Builds every route's pipeline once, so a missing registration fails at startup
    /// instead of failing every batch.</summary>
    internal void Verify(IServiceScopeFactory scopes, string componentName)
    {
        using var scope = scopes.CreateScope();
        foreach (var route in Routes)
            route.Verify(scope.ServiceProvider, componentName);
    }

    /// <summary>The route for <paramref name="message"/>, or null when none handles it.</summary>
    internal IBulkMessageRoute? Find(IncomingMessage message) =>
        _catchAll ?? Routes.FirstOrDefault(r => r.Matches(message));
}
