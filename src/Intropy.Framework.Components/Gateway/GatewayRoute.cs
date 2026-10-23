using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Gateway;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Intropy.Framework.Components.Gateway;

/// <summary>What one endpoint accepts, with its types bound.</summary>
internal interface IGatewayRoute
{
    /// <summary>The endpoint's port, as declared in the system's topology.</summary>
    string Port { get; }

    /// <summary>The path callers <c>POST</c> the endpoint's items to.</summary>
    string Path { get; }

    /// <summary>The route's deserialized request body.</summary>
    Type Input { get; }

    /// <summary>The route's published payload.</summary>
    Type Payload { get; }

    /// <summary>Registers the steps the route declared.</summary>
    void Register(IServiceCollection services);

    /// <summary>Declares the route as the hosting layer's endpoint it belongs to.</summary>
    void AddTo(GatewayEndpointDeclarations endpoints, IncidentRule defaultIncidents);
}

internal sealed class GatewayRoute<TInput, TPayload, TCtx>(string port, string path,
    GatewayPipelineDeclaration<TInput, TPayload, TCtx> pipeline, ContextFactory<TCtx> contextFactory)
    : IGatewayRoute where TCtx : Context
{
    public Type Input => typeof(TInput);

    public Type Payload => typeof(TPayload);

    public string Port => port;

    public string Path => path;

    private string Name => $"The pipeline of {port} ({typeof(TInput).Name} → {typeof(TPayload).Name})";

    public void Register(IServiceCollection services)
    {
        if (pipeline.DeclaredSteps is not { } steps)
            throw new InvalidOperationException(
                $"{Name} has no steps: call Steps(steps => steps.Deserialize<...>().Validate<...>().Transform<...>()) " +
                "on the pipeline.");
        // The classes themselves, transiently resolved from the request's scope, so a step keeps
        // its constructor injection. A class registered first — in ConfigureServices — stays the
        // component's.
        foreach (var step in steps.All)
            services.TryAddTransient(step);
        ComponentPublishing.AddPublisherOn<TCtx>(services);
    }

    public void AddTo(GatewayEndpointDeclarations endpoints, IncidentRule defaultIncidents) =>
        endpoints.Endpoint(port, path, contextFactory, pipeline.Pipeline(Name, defaultIncidents));
}
