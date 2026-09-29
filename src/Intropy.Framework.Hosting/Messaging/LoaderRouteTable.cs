namespace Intropy.Framework.Hosting.Messaging;

/// <summary>Finds the route for a message's CloudEvent type. Built once, at registration.</summary>
internal sealed class LoaderRouteTable
{
    private readonly Dictionary<string, ILoaderRoute> _byType;
    private readonly ILoaderRoute? _catchAll;

    internal LoaderRouteTable(IReadOnlyList<ILoaderRoute> routes)
    {
        if (routes.Count == 0)
            throw new ArgumentException("A loader needs at least one route.", nameof(routes));

        _catchAll = routes.SingleOrDefault(r => r.EventType is null);
        if (_catchAll is not null && routes.Count > 1)
            throw new ArgumentException("A loader that handles every event type cannot also have typed routes.",
                nameof(routes));

        _byType = routes.Where(r => r.EventType is not null)
            .ToDictionary(r => r.EventType!, StringComparer.Ordinal);
        Routes = routes;
    }

    /// <summary>Every route, in registration order.</summary>
    internal IReadOnlyList<ILoaderRoute> Routes { get; }

    /// <summary>Whether any route runs batches: the loader then receives through Dapr bulk subscribe.</summary>
    internal bool HasBatchRoutes => Routes.Any(r => r.IsBatch);

    /// <summary>Whether the loader routes by event type (and so can receive unrouted messages).</summary>
    internal bool IsRouting => _catchAll is null;

    /// <summary>The route for <paramref name="eventType"/>, or null when none handles it.</summary>
    internal ILoaderRoute? Find(string? eventType) =>
        _catchAll ?? (eventType is not null && _byType.TryGetValue(eventType, out var route) ? route : null);
}
