using Intropy.Framework.Blocks.BulkLoader;
using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// The routes of a routing bulk loader: one bulk loader pipeline per CloudEvent type consumed from
/// the loader's topic. Each route is typed on its own contract, builds its pipeline in the batch's
/// DI scope, and keeps its idempotency records apart from the other routes by default
/// (<see cref="IdempotencyScope.Route"/>).
/// </summary>
/// <remarks>
/// Bulk delivery is a property of the whole subscription, not of one of its routing rules, so every
/// route of a bulk loader is delivered in batches — a type that rarely has more than one message
/// pending simply arrives in batches of one.
/// </remarks>
public sealed class BulkLoaderRoutes
{
    private readonly List<IBulkMessageRoute> _routes = [];

    /// <summary>Routes messages of CloudEvent type <paramref name="eventType"/> to a bulk loader
    /// pipeline.</summary>
    /// <param name="eventType">The CloudEvent <c>type</c> this route handles, matched exactly.</param>
    /// <param name="configurePipeline">Configures the route's pipeline builder, given the batch's
    /// scope.</param>
    /// <param name="contextFactory">Creates the context for each message of a batch.</param>
    /// <typeparam name="TInput">The route's deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the route sends to the external system.</typeparam>
    /// <typeparam name="TCtx">The route's pipeline context.</typeparam>
    /// <returns>The routes, for chaining.</returns>
    /// <exception cref="ArgumentException">A route for <paramref name="eventType"/> is already
    /// registered.</exception>
    public BulkLoaderRoutes On<TInput, TOutput, TCtx>(string eventType,
        Func<BulkLoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, BulkLoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory) where TCtx : Context
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventType);
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(contextFactory);
        if (_routes.Any(r => r.IsCatchAll))
            throw new ArgumentException("A bulk loader that handles every event type cannot also have typed routes.",
                nameof(eventType));
        if (_routes.Any(r => string.Equals(r.Name, eventType, StringComparison.Ordinal)))
            throw new ArgumentException($"A route for event type '{eventType}' is already registered.",
                nameof(eventType));

        _routes.Add(new BulkLoaderRoute<TInput, TOutput, TCtx>(eventType, configurePipeline, contextFactory));
        return this;
    }

    /// <summary>The single route of a bulk loader without routes: it handles every event type.</summary>
    /// <remarks>
    /// A bulk loader either has one route handling every event type or typed routes — the two
    /// shapes cannot be mixed, and the route table enforces the same rule when it is built.
    /// </remarks>
    /// <param name="configurePipeline">Configures the route's pipeline builder, given the batch's
    /// scope.</param>
    /// <param name="contextFactory">Creates the context for each message of a batch.</param>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the loader sends to the external system.</typeparam>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <returns>The routes, for chaining.</returns>
    /// <exception cref="ArgumentException">An every-event-type route is already registered, or typed
    /// routes are.</exception>
    public BulkLoaderRoutes OnAny<TInput, TOutput, TCtx>(
        Func<BulkLoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, BulkLoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(contextFactory);
        switch (_routes.FirstOrDefault())
        {
            case null:
                break;
            case { IsCatchAll: true }:
                throw new ArgumentException("An every-event-type route is already registered; a bulk loader handles " +
                    "its topic either with one pipeline or with typed routes, not two catch-alls.");
            default:
                throw new ArgumentException("A bulk loader that handles every event type cannot also have typed routes.");
        }

        _routes.Add(new BulkLoaderRoute<TInput, TOutput, TCtx>(null, configurePipeline, contextFactory));
        return this;
    }

    /// <summary>The single route of a bulk loader without routes: it handles every event type.</summary>
    internal static BulkLoaderRoutes Any<TInput, TOutput, TCtx>(
        Func<BulkLoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, BulkLoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(contextFactory);
        var routes = new BulkLoaderRoutes();
        routes._routes.Add(new BulkLoaderRoute<TInput, TOutput, TCtx>(null, configurePipeline, contextFactory));
        return routes;
    }

    internal IReadOnlyList<IBulkMessageRoute> Routes => _routes;
}
