using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Gateway;

namespace Intropy.Framework.Components.Gateway;

/// <summary>
/// What a gateway component accepts, on which endpoint, and what it publishes: for each endpoint,
/// the kind of data callers post to it, and the pipeline — steps and rules — that turns it into the
/// component's one message.
/// </summary>
/// <remarks>
/// <code>
/// inputs =&gt; inputs
///     .Accepts&lt;OrderRequest&gt;()
///         .Publishes&lt;OrderPlaced&gt;(pipeline =&gt; pipeline.Steps(...).Idempotency(...).OutputEvent(...))
///     .On("order-gateway-edi", "/orders/edi")
///         .Accepts&lt;EdiOrder&gt;()
///         .Publishes&lt;OrderPlaced&gt;(pipeline =&gt; pipeline.Steps(...).Idempotency(...).OutputEvent(...))
/// </code>
/// <see cref="Accepts{TInput}"/> declares the endpoint the component's manifest names;
/// <see cref="On"/> names another. An endpoint accepts exactly one kind of data, and every endpoint
/// publishes the same type: the projection of the component's message. Two endpoints accepting the
/// same input type may name the same steps or their own — each pipeline's <c>Steps</c> says which
/// classes it runs.
/// </remarks>
public sealed class GatewayInputs
{
    private readonly List<GatewayEndpointInput> _endpoints = [];
    private readonly Func<GatewayEndpointDeclaration> _recorded;

    /// <param name="recorded">The endpoint the component's manifest names, read only by
    /// <see cref="Accepts{TInput}"/>.</param>
    internal GatewayInputs(Func<GatewayEndpointDeclaration> recorded) => _recorded = recorded;

    /// <summary>Accepts <typeparamref name="TInput"/> on the gateway's endpoint as the system declares
    /// it — the port and path the component's manifest names (<see cref="ComponentManifest.Port"/> at
    /// <see cref="ComponentManifest.EndpointPath"/>, else <c>/&lt;publishes&gt;</c>).</summary>
    /// <typeparam name="TInput">The deserialized request body.</typeparam>
    /// <returns>The input, to say what it publishes.</returns>
    /// <exception cref="InvalidOperationException">The manifest names no endpoint, or the endpoint is
    /// already declared.</exception>
    public GatewayInput<TInput> Accepts<TInput>()
    {
        var recorded = _recorded();
        return On(recorded.Port, recorded.Path).Accepts<TInput>();
    }

    /// <summary>Names an endpoint of the component's own, to say what it accepts.</summary>
    /// <param name="port">The endpoint's name, as declared in the system's topology.</param>
    /// <param name="path">The path callers <c>POST</c> items to, starting with <c>/</c>.</param>
    /// <returns>The endpoint, to say what it accepts.</returns>
    /// <exception cref="ArgumentException">The port or path is missing.</exception>
    /// <exception cref="InvalidOperationException">The endpoint is already declared.</exception>
    public GatewayEndpointInput On(string port, string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        if (_endpoints.FirstOrDefault(e => string.Equals(e.Port, port, StringComparison.Ordinal)) is { } declared)
            throw new InvalidOperationException(declared.Input is { } input
                ? $"Endpoint '{port}' already accepts {input.Name}: an endpoint takes one kind of data, " +
                  "so declare another endpoint for the other."
                : $"Endpoint '{port}' is already declared.");
        var endpoint = new GatewayEndpointInput(this, port, path);
        _endpoints.Add(endpoint);
        return endpoint;
    }

    /// <summary>Each endpoint's route, in declaration order.</summary>
    /// <exception cref="InvalidOperationException">An endpoint accepts or publishes nothing, or the
    /// endpoints publish different types.</exception>
    internal IReadOnlyList<IGatewayRoute> Routes
    {
        get
        {
            var routes = _endpoints.Select(e => e.Route ?? throw new InvalidOperationException(e.Input is { } input
                ? $"Endpoint '{e.Port}' accepts {input.Name} but publishes nothing: call Publishes<TPayload>(...) on it."
                : $"Endpoint '{e.Port}' accepts nothing: call Accepts<TInput>() on it.")).ToList();
            var payloads = routes.Select(r => r.Payload).Distinct().ToList();
            if (payloads.Count > 1)
                throw new InvalidOperationException(
                    $"The endpoints publish {string.Join(", ", payloads.Select(p => p.Name))}: a gateway publishes " +
                    "its one message, so every endpoint publishes the same type.");
            return routes;
        }
    }
}

/// <summary>One endpoint of a gateway component, being declared: what it accepts.</summary>
public sealed class GatewayEndpointInput
{
    internal GatewayEndpointInput(GatewayInputs inputs, string port, string path) =>
        (Inputs, Port, Path) = (inputs, port, path);

    internal GatewayInputs Inputs { get; }

    internal string Port { get; }

    internal string Path { get; }

    internal Type? Input { get; private set; }

    internal IGatewayRoute? Route { get; set; }

    /// <summary>Accepts <typeparamref name="TInput"/> on this endpoint.</summary>
    /// <typeparam name="TInput">The deserialized request body.</typeparam>
    /// <returns>The input, to say what it publishes.</returns>
    /// <exception cref="InvalidOperationException">The endpoint already accepts something.</exception>
    public GatewayInput<TInput> Accepts<TInput>()
    {
        if (Input is not null)
            throw new InvalidOperationException(
                $"Endpoint '{Port}' already accepts {Input.Name}: an endpoint takes one kind of data, " +
                "so declare another endpoint for the other.");
        Input = typeof(TInput);
        return new GatewayInput<TInput>(this);
    }
}

/// <summary>What one endpoint accepts, being declared: the context its items run on, what it
/// publishes, and through which pipeline.</summary>
/// <typeparam name="TInput">The deserialized request body.</typeparam>
public sealed class GatewayInput<TInput>
{
    private readonly GatewayEndpointInput _endpoint;

    internal GatewayInput(GatewayEndpointInput endpoint) => _endpoint = endpoint;

    /// <summary>Runs each accepted item on a context the component creates — a
    /// <see cref="Context"/> of its own type, carrying what its steps share. Optional: without it
    /// the pipeline runs on <see cref="Context"/>. The gateway's own metadata — the port, the
    /// request id, the time received — is on the context whatever the factory does.</summary>
    /// <param name="contextFactory">Creates each request's context from the gateway's metadata,
    /// e.g. <c>(metadata, isRetry) =&gt; new OrderContext(metadata, isRetry)</c>.</param>
    /// <typeparam name="TCtx">The context type; the pipeline's steps and rules are typed on
    /// it.</typeparam>
    /// <returns>The input, to say what it publishes.</returns>
    public GatewayInput<TInput, TCtx> WithContext<TCtx>(ContextFactory<TCtx> contextFactory) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(contextFactory);
        return new GatewayInput<TInput, TCtx>(_endpoint, contextFactory);
    }

    /// <summary>Publishes each accepted <typeparamref name="TInput"/> as <typeparamref name="TPayload"/>,
    /// through the steps and rules its pipeline declares, on a <see cref="Context"/>.</summary>
    /// <param name="configure">Declares the pipeline's steps and rules.</param>
    /// <typeparam name="TPayload">The published payload: the projection of the component's
    /// message.</typeparam>
    /// <returns>The inputs, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The input already publishes.</exception>
    public GatewayInputs Publishes<TPayload>(Action<GatewayPipelineDeclaration<TInput, TPayload, Context>> configure) =>
        new GatewayInput<TInput, Context>(_endpoint, static (metadata, isRetry) => new Context(metadata, isRetry))
            .Publishes(configure);
}

/// <summary>What one endpoint accepts, on the component's own context type, being declared: what it
/// publishes, and through which pipeline.</summary>
/// <typeparam name="TInput">The deserialized request body.</typeparam>
/// <typeparam name="TCtx">The pipeline's context type.</typeparam>
public sealed class GatewayInput<TInput, TCtx>
    where TCtx : Context
{
    private readonly GatewayEndpointInput _endpoint;
    private readonly ContextFactory<TCtx> _contextFactory;

    internal GatewayInput(GatewayEndpointInput endpoint, ContextFactory<TCtx> contextFactory) =>
        (_endpoint, _contextFactory) = (endpoint, contextFactory);

    /// <summary>Publishes each accepted <typeparamref name="TInput"/> as <typeparamref name="TPayload"/>,
    /// through the steps and rules its pipeline declares.</summary>
    /// <param name="configure">Declares the pipeline's steps and rules.</param>
    /// <typeparam name="TPayload">The published payload: the projection of the component's
    /// message.</typeparam>
    /// <returns>The inputs, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The input already publishes.</exception>
    public GatewayInputs Publishes<TPayload>(Action<GatewayPipelineDeclaration<TInput, TPayload, TCtx>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        if (_endpoint.Route is not null)
            throw new InvalidOperationException(
                $"Endpoint '{_endpoint.Port}' already publishes {_endpoint.Route.Payload.Name}.");
        var pipeline = new GatewayPipelineDeclaration<TInput, TPayload, TCtx>();
        configure(pipeline);
        _endpoint.Route = new GatewayRoute<TInput, TPayload, TCtx>(_endpoint.Port, _endpoint.Path, pipeline, _contextFactory);
        return _endpoint.Inputs;
    }
}
