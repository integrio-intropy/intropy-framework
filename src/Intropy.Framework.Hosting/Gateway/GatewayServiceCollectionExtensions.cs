using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// Registers and maps a gateway: an extractor whose items are pushed to it over HTTP.
/// </summary>
public static class GatewayServiceCollectionExtensions
{
    /// <summary>The liveness probe's path: answers <c>200</c> while the process serves requests.</summary>
    public const string LivenessPath = "/healthz";

    /// <summary>The readiness probe's path: answers <c>200</c> once the sidecar is available, and
    /// <c>503</c> until then.</summary>
    public const string ReadinessPath = "/readyz";

    /// <summary>
    /// Registers a gateway: one endpoint per declared port, each its pipeline, its startup
    /// (pipeline composition and the sidecar wait) and its options. Map it with
    /// <see cref="MapGateway"/>. Requires <c>AddIntropyFramework</c> and the services the
    /// pipelines resolve — the idempotency and business-incident clients and the sender.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="definition">The gateway.</param>
    /// <returns>The service collection, for chaining.</returns>
    /// <exception cref="ArgumentException">An endpoint's declaration is invalid.</exception>
    /// <exception cref="InvalidOperationException">No endpoint is declared, the options are
    /// invalid, or a gateway is already registered.</exception>
    public static IServiceCollection AddGateway(this IServiceCollection services, GatewayDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(definition);
        ComponentRegistration.EnsureSingleKind(services, "gateway");
        if (definition.Endpoints is null)
            throw new InvalidOperationException(
                "Gateway composition failed: the definition needs Endpoints.");

        var endpoints = new GatewayEndpointDeclarations();
        definition.Endpoints(endpoints);
        var declared = endpoints.Declared;

        var options = new GatewayOptions();
        definition.ConfigureOptions?.Invoke(options);
        options.Validate();
        ComponentRegistration.MarkRegistered(services, "gateway");

        var respond = definition.Respond ?? GatewayResponses.Default;
        services.AddSingleton(options);
        services.AddSingleton<GatewayReadiness>();
        services.AddSingleton(provider => new GatewayEndpointSet(
            declared.Select(e => new GatewayEndpointHandler(
                e.Pipeline,
                e.Declaration.Port,
                e.Declaration.Path,
                respond,
                options,
                provider.GetRequiredService<GatewayReadiness>(),
                provider.GetRequiredService<FrameworkOptions>(),
                provider.GetService<TimeProvider>() ?? TimeProvider.System,
                provider.GetRequiredService<ILogger<GatewayEndpointHandler>>())).ToList(),
            definition.ConfigureEndpoints));
        services.AddHostedService<GatewayStartup>();
        return services;
    }

    /// <summary>
    /// Maps the registered gateway: <c>POST</c> on each endpoint's path, as one group, and the
    /// liveness and readiness probes outside it.
    /// </summary>
    /// <param name="endpoints">The application's endpoint route builder.</param>
    /// <returns>The group of the gateway's endpoints, for further conventions.</returns>
    /// <exception cref="InvalidOperationException">No gateway is registered.</exception>
    public static IEndpointConventionBuilder MapGateway(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        var gateway = endpoints.ServiceProvider.GetService<GatewayEndpointSet>() ?? throw new InvalidOperationException(
            "No gateway is registered: call services.AddGateway(...) before app.MapGateway().");
        var readiness = endpoints.ServiceProvider.GetRequiredService<GatewayReadiness>();

        endpoints.MapGet(LivenessPath, () => Results.Ok());
        endpoints.MapGet(ReadinessPath, () => readiness.IsReady
            ? Results.Ok()
            : Results.StatusCode(StatusCodes.Status503ServiceUnavailable));

        var group = endpoints.MapGroup("");
        foreach (var endpoint in gateway.All)
            // A RequestDelegate, not a handler returning IResult: the endpoint writes its own response.
            group.MapPost(endpoint.Path, (RequestDelegate)endpoint.HandleAsync)
                .WithName($"{endpoint.ComponentName}:{endpoint.Port}");
        gateway.ConfigureEndpoints?.Invoke(group);
        return group;
    }
}

/// <summary>The gateway's endpoints, in declaration order, and the conventions for their group.</summary>
internal sealed record GatewayEndpointSet(
    IReadOnlyList<GatewayEndpointHandler> All,
    Action<IEndpointConventionBuilder>? ConfigureEndpoints);
