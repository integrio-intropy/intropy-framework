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

    /// <summary>Checks the route's payload and registers the steps it declared.</summary>
    void Register(IServiceCollection services, ComponentManifest manifest);

    /// <summary>Declares the route as the hosting layer's endpoint it belongs to.</summary>
    void AddTo(GatewayEndpointDeclarations endpoints, IncidentRule defaultIncidents);
}

internal sealed class GatewayRoute<TInput, TOutput>(string port, string path, PublishingRoute<TInput, TOutput> route)
    : IGatewayRoute
{
    public Type Input => typeof(TInput);

    public string Port => port;

    public string Path => path;

    private string Name => $"The route of {port} ({typeof(TInput).Name} → {typeof(TOutput).Name})";

    public void Register(IServiceCollection services, ComponentManifest manifest)
    {
        ComponentPublishing.EnsurePayload(typeof(TOutput), manifest, Name);
        if (route.DeclaredSteps is not { } steps)
            throw new InvalidOperationException(
                $"{Name} has no steps: call Steps<TDeserializer, TValidator, TTransformer>() on the route.");
        // The classes themselves, transiently resolved from the request's scope, so a step keeps
        // its constructor injection. A class registered first — in ConfigureServices — stays the
        // component's; the slot (the step base type) is the component's to register, not a route's:
        // two endpoints accepting the same input type may name different classes.
        services.TryAddTransient(steps.Deserializer);
        services.TryAddTransient(steps.Validator);
        services.TryAddTransient(steps.Transformer);
    }

    public void AddTo(GatewayEndpointDeclarations endpoints, IncidentRule defaultIncidents) =>
        endpoints.Endpoint(port, path, route.Pipeline(Name, defaultIncidents));
}
