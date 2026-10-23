using Intropy.Framework.Hosting.Extractor;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// Describes a gateway for <see cref="GatewayServiceCollectionExtensions.AddGateway"/>: an
/// extractor whose items are pushed to it over HTTP instead of swept from a source. Its endpoints
/// take the place of an extractor's source ports; each is declared as one unit — its port, its
/// path, and the pipeline its items go through — and each request body runs through that pipeline
/// as one item, and the pipeline's result is the caller's answer.
/// </summary>
/// <remarks>
/// <code>
/// services.AddGateway(new GatewayDefinition
/// {
///     Endpoints = endpoints =&gt; endpoints
///         .Endpoint&lt;WebshopOrder, OrderPlaced&gt;("order-gateway-webshop", "/orders/webshop", (builder, sp) =&gt; builder...)
///         .Endpoint&lt;EdiOrder, OrderPlaced&gt;("order-gateway-edi", "/orders/edi", (builder, sp) =&gt; builder...),
/// });
/// ...
/// app.MapGateway();
/// </code>
/// </remarks>
public sealed class GatewayDefinition
{
    /// <summary>Declares the endpoints the gateway exposes, each with the pipeline its items go
    /// through. A body reaches the pipeline's deserializer as undecoded bytes.</summary>
    public required Action<GatewayEndpointDeclarations> Endpoints { get; init; }

    /// <summary>Maps an outcome to the response. Default: <see cref="GatewayResponses.Default"/>.</summary>
    public Func<GatewayOutcome, IResult>? Respond { get; init; }

    /// <summary>Configures the gateway's endpoints — authorization, rate limiting, OpenAPI
    /// metadata — as one group. Default: none; the environment's ingress owns authentication.</summary>
    public Action<IEndpointConventionBuilder>? ConfigureEndpoints { get; init; }

    /// <summary>Adjusts the <see cref="GatewayOptions"/>.</summary>
    public Action<GatewayOptions>? ConfigureOptions { get; init; }
}
