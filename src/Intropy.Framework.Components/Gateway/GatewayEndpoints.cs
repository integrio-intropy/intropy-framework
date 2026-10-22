using Intropy.Framework.Hosting.Gateway;

namespace Intropy.Framework.Components.Gateway;

/// <summary>
/// The endpoints of a gateway component, each with what it accepts: the kind of data callers post
/// to it, its steps, and the rules of the pipeline that data goes through. Every endpoint publishes
/// the component's one message.
/// </summary>
/// <remarks>
/// <code>
/// endpoints =&gt; endpoints
///     .Endpoint(endpoint =&gt; endpoint
///         .Accepts&lt;SourceOrder, OrderPlaced&gt;(route =&gt; route.Steps&lt;...&gt;(...).Idempotency(...).OutputEvent(...)))
///     .Endpoint("order-gateway-edi", "/orders/edi", endpoint =&gt; endpoint
///         .Accepts&lt;EdiOrder, OrderPlaced&gt;(route =&gt; route.Steps&lt;...&gt;(...).Idempotency(...).OutputEvent(...)))
/// </code>
/// An endpoint accepts exactly one kind of data. Two endpoints accepting the same input type may
/// name the same steps or their own — each route's <c>Steps</c> says whose pipeline it runs.
/// </remarks>
public sealed class GatewayEndpoints
{
    private readonly List<IGatewayRoute> _routes = [];
    private readonly Func<GatewayEndpointDeclaration> _recorded;

    /// <param name="recorded">The endpoint the component's identity names, read only by
    /// <see cref="Endpoint(Action{GatewayEndpoint})"/>.</param>
    internal GatewayEndpoints(Func<GatewayEndpointDeclaration> recorded) => _recorded = recorded;

    /// <summary>Declares the gateway's endpoint as the system declares it — the port and path the
    /// component's <see cref="IntropyComponentAttribute"/> names (<see cref="IntropyComponentAttribute.Port"/>
    /// at <see cref="IntropyComponentAttribute.EndpointPath"/>, else <c>/&lt;publishes&gt;</c>) — and what it
    /// accepts.</summary>
    /// <param name="configure">Says what the endpoint accepts.</param>
    /// <returns>The endpoints, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The <see cref="IntropyComponentAttribute"/> names no
    /// endpoint, or the endpoint accepts nothing.</exception>
    public GatewayEndpoints Endpoint(Action<GatewayEndpoint> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var recorded = _recorded();
        return Endpoint(recorded.Port, recorded.Path, configure);
    }

    /// <summary>Declares an endpoint and what it accepts.</summary>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="path">The path callers <c>POST</c> items to, starting with <c>/</c>.</param>
    /// <param name="configure">Says what the endpoint accepts.</param>
    /// <returns>The endpoints, for chaining.</returns>
    /// <exception cref="ArgumentException">The port or path is missing, or the path does not start
    /// with <c>/</c>.</exception>
    /// <exception cref="InvalidOperationException">The endpoint accepts nothing.</exception>
    public GatewayEndpoints Endpoint(string port, string path, Action<GatewayEndpoint> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var endpoint = new GatewayEndpoint(port, path);
        configure(endpoint);
        _routes.Add(endpoint.Route ?? throw new InvalidOperationException(
            $"Endpoint '{port}' accepts nothing: call Accepts<TInput, TOutput>(...) on it."));
        return this;
    }

    /// <summary>The declared endpoints, in declaration order. The endpoints' port and path rules —
    /// a path starting with <c>/</c>, no duplicate port or path — are the hosting layer's; they are
    /// checked when the gateway registers.</summary>
    internal IReadOnlyList<GatewayEndpointDeclaration> Declared =>
        _routes.Select(r => new GatewayEndpointDeclaration(r.Port, r.Path)).ToList();

    /// <summary>Each endpoint's route, in declaration order.</summary>
    internal IReadOnlyList<IGatewayRoute> Routes => _routes;
}

/// <summary>One endpoint of a gateway component, being declared: what it accepts.</summary>
public sealed class GatewayEndpoint
{
    private readonly string _port;
    private readonly string _path;

    internal GatewayEndpoint(string port, string path) => (_port, _path) = (port, path);

    internal IGatewayRoute? Route { get; private set; }

    /// <summary>Accepts <typeparamref name="TInput"/> and publishes it as
    /// <typeparamref name="TOutput"/>, through the steps and rules the route names.</summary>
    /// <param name="configure">Sets the route's steps and rules.</param>
    /// <typeparam name="TInput">The deserialized request body.</typeparam>
    /// <typeparam name="TOutput">The published payload: the projection of the component's message.</typeparam>
    /// <exception cref="InvalidOperationException">The endpoint already accepts something.</exception>
    public void Accepts<TInput, TOutput>(Action<PublishingRoute<TInput, TOutput>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        if (Route is not null)
            throw new InvalidOperationException(
                $"Endpoint '{_port}' already accepts {Route.Input.Name}: an endpoint takes one kind of data, " +
                "so declare another endpoint for the other.");
        var route = new PublishingRoute<TInput, TOutput>();
        configure(route);
        Route = new GatewayRoute<TInput, TOutput>(_port, _path, route);
    }
}
