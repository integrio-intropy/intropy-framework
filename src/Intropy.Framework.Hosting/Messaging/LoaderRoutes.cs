using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// The routes of a routing loader: one loader pipeline per CloudEvent type consumed from the
/// loader's topic. Each route is typed on its own contract, builds its pipeline in the message's
/// DI scope, and keeps its idempotency records apart from the other routes by default
/// (<see cref="IdempotencyScope.Route"/>).
/// </summary>
public sealed class LoaderRoutes
{
    private readonly List<ILoaderRoute> _routes = [];

    /// <summary>Routes messages of CloudEvent type <paramref name="eventType"/> to a loader
    /// pipeline.</summary>
    /// <param name="eventType">The CloudEvent <c>type</c> this route handles, matched exactly.</param>
    /// <param name="configurePipeline">Configures the route's pipeline builder, given the message's
    /// scope.</param>
    /// <param name="contextFactory">Creates the context for each message.</param>
    /// <typeparam name="TInput">The route's deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the route sends to the external system.</typeparam>
    /// <typeparam name="TCtx">The route's pipeline context.</typeparam>
    /// <returns>The routes, for chaining.</returns>
    /// <exception cref="ArgumentException">A route for <paramref name="eventType"/> is already
    /// registered.</exception>
    public LoaderRoutes On<TInput, TOutput, TCtx>(string eventType,
        Func<LoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, LoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory) where TCtx : Context
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventType);
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(contextFactory);
        if (_routes.Any(r => string.Equals(r.EventType, eventType, StringComparison.Ordinal)))
            throw new ArgumentException($"A route for event type '{eventType}' is already registered.", nameof(eventType));

        _routes.Add(new LoaderRoute<TInput, TOutput, TCtx>(eventType, configurePipeline, contextFactory));
        return this;
    }

    /// <summary>
    /// Routes messages of CloudEvent type <paramref name="eventType"/> to a batch pipeline: the
    /// loader then receives in batches (Dapr bulk subscribe), and the route runs each delivered batch
    /// at once — filter, coalesce by entity key, look up per chunk, then per entity.
    /// </summary>
    /// <param name="eventType">The CloudEvent <c>type</c> this route handles, matched exactly.</param>
    /// <param name="configurePipeline">Configures the route's batch pipeline builder, given the
    /// batch's scope.</param>
    /// <param name="contextFactory">Creates the context for each entry.</param>
    /// <typeparam name="TInput">The route's deserialized event.</typeparam>
    /// <typeparam name="TEnriched">What the per-entity steps run on: the looked-up state, or
    /// <typeparamref name="TInput"/> without a lookup.</typeparam>
    /// <typeparam name="TOutput">What the route sends to the external system.</typeparam>
    /// <typeparam name="TCtx">The route's pipeline context.</typeparam>
    /// <returns>The routes, for chaining.</returns>
    /// <exception cref="ArgumentException">A route for <paramref name="eventType"/> is already
    /// registered.</exception>
    public LoaderRoutes OnBatch<TInput, TEnriched, TOutput, TCtx>(string eventType,
        Func<BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>, IServiceProvider, BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory) where TCtx : Context
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventType);
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(contextFactory);
        if (_routes.Any(r => string.Equals(r.EventType, eventType, StringComparison.Ordinal)))
            throw new ArgumentException($"A route for event type '{eventType}' is already registered.", nameof(eventType));

        _routes.Add(new BatchLoaderRoute<TInput, TEnriched, TOutput, TCtx>(eventType, configurePipeline, contextFactory));
        return this;
    }

    /// <summary>The single batch route of a batch loader without routes: it handles every event type.</summary>
    internal static LoaderRoutes AnyBatch<TInput, TEnriched, TOutput, TCtx>(
        Func<BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>, IServiceProvider, BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(contextFactory);
        var routes = new LoaderRoutes();
        routes._routes.Add(new BatchLoaderRoute<TInput, TEnriched, TOutput, TCtx>(null, configurePipeline, contextFactory));
        return routes;
    }

    /// <summary>The single route of a loader without routes: it handles every event type.</summary>
    internal static LoaderRoutes Any<TInput, TOutput, TCtx>(
        Func<LoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, LoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(contextFactory);
        var routes = new LoaderRoutes();
        routes._routes.Add(new LoaderRoute<TInput, TOutput, TCtx>(null, configurePipeline, contextFactory));
        return routes;
    }

    internal IReadOnlyList<ILoaderRoute> Routes => _routes;
}
