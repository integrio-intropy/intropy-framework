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
        if (_routes.Any(r => r.EventType is null))
            throw new ArgumentException("A loader that handles every event type cannot also have typed routes.",
                nameof(eventType));
        if (_routes.Any(r => string.Equals(r.EventType, eventType, StringComparison.Ordinal)))
            throw new ArgumentException($"A route for event type '{eventType}' is already registered.", nameof(eventType));

        _routes.Add(new LoaderRoute<TInput, TOutput, TCtx>(eventType, configurePipeline, contextFactory));
        return this;
    }

    /// <summary>The single route of a loader without routes: it handles every event type. Use it
    /// through the routed overload when the pipeline-per-event-type shape is declared in one place
    /// and the catch-all falls out of the same <c>LoaderRoutes</c> block:
    /// <code>
    /// services.AddLoader(options =&gt; { ... }, routes =&gt; routes
    ///     .OnAny&lt;OrderCreated, OrderCreated, Context&gt;(ConfigurePipeline, contextFactory));
    /// </code>
    /// </summary>
    /// <remarks>
    /// A loader either has one route handling every event type or typed routes — the two shapes
    /// cannot be mixed, and the route table enforces the same rule when it is built.
    /// </remarks>
    /// <param name="configurePipeline">Configures the route's pipeline builder, given the message's
    /// scope.</param>
    /// <param name="contextFactory">Creates the context for each message.</param>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">What the loader sends to the external system.</typeparam>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <returns>The routes, for chaining.</returns>
    /// <exception cref="ArgumentException">An every-event-type route is already registered, or
    /// typed routes are.</exception>
    public LoaderRoutes OnAny<TInput, TOutput, TCtx>(
        Func<LoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, LoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(contextFactory);
        switch (_routes.FirstOrDefault())
        {
            case null:
                break;
            case { EventType: null }:
                throw new ArgumentException("An every-event-type route is already registered; a loader handles " +
                    "its topic either with one pipeline or with typed routes, not two catch-alls.");
            default:
                throw new ArgumentException("A loader that handles every event type cannot also have typed routes.");
        }

        _routes.Add(new LoaderRoute<TInput, TOutput, TCtx>(null, configurePipeline, contextFactory));
        return this;
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
