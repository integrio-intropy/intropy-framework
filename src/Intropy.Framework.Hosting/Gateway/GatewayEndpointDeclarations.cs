using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.FileSweeps;

namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// The endpoints a gateway exposes: each a port, as the system's topology names it, the path
/// callers <c>POST</c> its items to, and the one pipeline those items go through. The endpoint is
/// the gateway's counterpart of the source port an extractor sweeps; its pipeline the counterpart
/// of the port's route.
/// </summary>
/// <remarks>
/// <code>
/// endpoints =&gt; endpoints
///     .Endpoint&lt;WebshopOrder, OrderPlaced&gt;("order-gateway-webshop", "/orders/webshop", Configure)
///     .Endpoint&lt;EdiOrder, OrderPlaced&gt;("order-gateway-edi", "/orders/edi", ConfigureEdi)
/// </code>
/// The port is the endpoint's identity: it is stamped on every event its items publish and scopes
/// their idempotency records. The path is its contract with callers, the same in every
/// environment; the host name in front of it is the environment's.
/// </remarks>
public sealed class GatewayEndpointDeclarations
{
    private readonly List<GatewayEndpoint> _endpoints = [];

    /// <summary>Declares an endpoint and the pipeline its items go through.</summary>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="path">The path callers <c>POST</c> items to, starting with <c>/</c>.</param>
    /// <param name="configurePipeline">Configures the endpoint's pipeline builder, given the
    /// request's scope — the same shape as <c>AddExtractor</c>'s route configuration.</param>
    /// <typeparam name="TInput">The pipeline's deserialized input.</typeparam>
    /// <typeparam name="TOutput">The pipeline's published payload.</typeparam>
    /// <returns>The endpoints, for chaining.</returns>
    /// <exception cref="ArgumentException">The port or path is missing, the path does not start
    /// with <c>/</c>, the port or path is already declared, or the pipeline is missing.</exception>
    public GatewayEndpointDeclarations Endpoint<TInput, TOutput>(string port, string path,
        Func<ExtractorBuilder<TInput, TOutput, Context>, IServiceProvider, ExtractorBuilder<TInput, TOutput, Context>> configurePipeline)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        if (!path.StartsWith('/'))
            throw new ArgumentException($"The path of endpoint '{port}' is '{path}'; a path starts with '/'.", nameof(path));
        if (_endpoints.Any(e => string.Equals(e.Declaration.Port, port, StringComparison.Ordinal)))
            throw new ArgumentException($"Endpoint '{port}' is already declared.", nameof(port));
        // ASP.NET Core matches paths case-insensitively, so two paths differing in case collide.
        if (_endpoints.FirstOrDefault(e => string.Equals(e.Declaration.Path, path, StringComparison.OrdinalIgnoreCase)) is { } taken)
            throw new ArgumentException($"Path '{path}' is already the path of endpoint '{taken.Declaration.Port}'.", nameof(path));
        ArgumentNullException.ThrowIfNull(configurePipeline);
        _endpoints.Add(new GatewayEndpoint(new GatewayEndpointDeclaration(port, path),
            new DeclaredExtractorRoute<TInput, TOutput, Context>(new FileMatch([port]), configurePipeline)));
        return this;
    }

    /// <summary>The declared endpoints, in declaration order. Registration requires at least one;
    /// an endpoint without its pipeline cannot be declared.</summary>
    /// <exception cref="InvalidOperationException">No endpoint is declared.</exception>
    internal IReadOnlyList<GatewayEndpoint> Declared => _endpoints.Count > 0
        ? _endpoints
        : throw new InvalidOperationException(
            "Gateway composition failed: no endpoint is declared; declare one with Endpoint.");
}

/// <summary>One declared endpoint: its port and its path.</summary>
/// <param name="Port">The port name, as declared in the system's topology.</param>
/// <param name="Path">The path callers <c>POST</c> items to.</param>
public sealed record GatewayEndpointDeclaration(string Port, string Path);

/// <summary>One declared endpoint, with the pipeline its items go through.</summary>
/// <param name="Declaration">The endpoint's port and path.</param>
/// <param name="Route">The pipeline the endpoint's items go through.</param>
internal sealed record GatewayEndpoint(GatewayEndpointDeclaration Declaration, ExtractorRoute<Context> Route);
