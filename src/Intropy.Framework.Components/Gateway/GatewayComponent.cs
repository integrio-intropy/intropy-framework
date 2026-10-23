using Intropy.Framework.Core.Common;
using Intropy.Framework.Hosting.Gateway;
using Intropy.Telemetry;
using Intropy.Telemetry.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Components.Gateway;

/// <summary>
/// A gateway: a resident HTTP service that accepts items pushed to its endpoints and publishes each
/// as a CloudEvent of the component's message through the extractor pipeline. It is an extractor
/// whose sources are its callers: endpoints take the place of source ports, and what each endpoint
/// accepts carries the pipeline its items go through. The component declares its identity, what it
/// accepts on each endpoint and what it publishes, with the steps and rules in between; the
/// framework wires the web host, the probes, telemetry, the platform clients and the publisher.
/// </summary>
/// <remarks>
/// <code>
/// public sealed class OrderGateway : GatewayComponent
/// {
///     protected override ComponentManifest Manifest { get; } = new()
///     {
///         ComponentName = "order-gateway", Organization = "Acme", Publishes = "order-placed",
///         EventSource = "urn:Acme:order-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
///     };
///
///     protected override void Inputs(GatewayInputs inputs) =&gt; inputs
///         .Accepts&lt;OrderRequest&gt;()
///         .Publishes&lt;OrderPlaced&gt;(pipeline =&gt; pipeline
///             .Steps(steps =&gt; steps
///                 .Deserialize&lt;OrderDeserializer&gt;()
///                 .Validate&lt;OrderValidator&gt;()
///                 .Transform&lt;OrderTransformer&gt;())
///             .Idempotency(key: (o, _) =&gt; o.OrderId, at: (o, _) =&gt; o.OrderDate)
///             .OutputEvent(subject: o =&gt; o.OrderId, at: o =&gt; o.OrderDate));
/// }
/// </code>
/// <para>
/// The manifest names the endpoint the system declares (<see cref="ComponentManifest.Port"/>,
/// <see cref="ComponentManifest.EndpointPath"/>): startup fails unless the component declares that
/// endpoint with that path, so the identity and the declared endpoints cannot drift apart.
/// <see cref="GatewayInputs.Accepts{TInput}"/> declares exactly that endpoint;
/// <see cref="GatewayInputs.On"/> names further endpoints of the component's own, each with its port
/// and path. Every endpoint publishes the same type, the projection of the component's message.
/// </para>
/// <para>
/// Every other default is a member to override: <see cref="Respond"/>,
/// <see cref="ConfigureEndpoints"/> (authorization, rate limiting),
/// <see cref="ConfigureApplication"/> (middleware),
/// <see cref="ConfigureOptions"/> (the <see cref="GatewayOptions"/> under
/// <c>Gateway</c> in configuration), and each pipeline's
/// <see cref="GatewayPipelineDeclaration{TInput,TPayload,TCtx}.Incidents"/>.
/// </para>
/// </remarks>
public abstract class GatewayComponent : IntropyComponent
{
    /// <summary>The port the host listens on when configuration names no URL or port.</summary>
    public const int DefaultHttpPort = 8080;

    /// <summary>Declares what the gateway accepts on each endpoint, and what it publishes.</summary>
    /// <param name="inputs">The inputs.</param>
    protected abstract void Inputs(GatewayInputs inputs);

    /// <summary>Maps a request's outcome to its response. Default:
    /// <see cref="GatewayResponses.Default"/>; an override may delegate the outcomes it keeps to
    /// it.</summary>
    /// <param name="outcome">The request's outcome.</param>
    /// <returns>The response.</returns>
    protected virtual IResult Respond(GatewayOutcome outcome) => GatewayResponses.Default(outcome);

    /// <summary>Configures the gateway's endpoints, as one group — e.g.
    /// <c>RequireAuthorization()</c>, <c>RequireRateLimiting(...)</c>. Default: none; the
    /// environment's ingress owns authentication.</summary>
    /// <param name="endpoints">The endpoints' convention builder.</param>
    protected virtual void ConfigureEndpoints(IEndpointConventionBuilder endpoints)
    {
    }

    /// <summary>Adds middleware before the endpoints are mapped — e.g. <c>UseAuthentication()</c>.
    /// Default: none.</summary>
    /// <param name="app">The application.</param>
    protected virtual void ConfigureApplication(WebApplication app)
    {
    }

    /// <summary>Adjusts the <see cref="GatewayOptions"/> after configuration has bound them.
    /// Default: none.</summary>
    /// <param name="options">The options.</param>
    protected virtual void ConfigureOptions(GatewayOptions options)
    {
    }

    /// <summary>The routes the component declares; <paramref name="named"/> is the endpoint the
    /// manifest names, read only when the component declares it without a port.</summary>
    private IReadOnlyList<IGatewayRoute> DeclareRoutes(Func<GatewayEndpointDeclaration> named)
    {
        var inputs = new GatewayInputs(named);
        Inputs(inputs);
        return inputs.Routes;
    }

    /// <inheritdoc />
    internal override async Task<int> RunAsync(string[] args)
    {
        // The binary's directory, not the working directory: a host may start the component from
        // anywhere, and appsettings.json sits next to the binary.
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            Args = args,
            ContentRootPath = AppContext.BaseDirectory,
        });
        if (string.IsNullOrEmpty(builder.Configuration[WebHostDefaults.ServerUrlsKey]) &&
            string.IsNullOrEmpty(builder.Configuration[WebHostDefaults.HttpPortsKey]))
            builder.WebHost.UseUrls($"http://+:{DefaultHttpPort}");

        await using var app = Build(builder);
        await app.RunAsync();
        return 0;
    }

    /// <summary>The endpoints the component declares, checked against the endpoint its manifest
    /// names.</summary>
    /// <exception cref="InvalidOperationException">The component does not declare the manifest's
    /// endpoint, or declares it at another path.</exception>
    internal IReadOnlyList<GatewayEndpointDeclaration> DeclareEndpoints() =>
        Declare().Select(r => new GatewayEndpointDeclaration(r.Port, r.Path)).ToList();

    private IReadOnlyList<IGatewayRoute> Declare()
    {
        var manifest = Manifest;
        var routes = DeclareRoutes(() => new GatewayEndpointDeclaration(
            manifest.Port ?? throw new InvalidOperationException(
                $"The manifest of {GetType().Name} names no Port: name the endpoint the system declares in " +
                "the manifest, or name the endpoint's port and path with On(port, path) in Inputs."),
            manifest.EndpointPath ?? $"/{manifest.Publishes}"));
        if (manifest.Port is not { } named)
            return routes;

        var endpoint = routes.FirstOrDefault(r => string.Equals(r.Port, named, StringComparison.Ordinal))
                       ?? throw new InvalidOperationException(
                           $"{GetType().Name} declares no endpoint '{named}', which its manifest names as the " +
                           $"gateway's port (it declares {string.Join(", ", routes.Select(r => $"'{r.Port}'"))}): " +
                           "the system declares the manifest's endpoint, so the component must expose it. Name the " +
                           "same port in the manifest and in Inputs.");
        if (manifest.EndpointPath is { } path && !string.Equals(endpoint.Path, path, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{GetType().Name} exposes endpoint '{named}' at '{endpoint.Path}', but its manifest names " +
                $"'{path}' (EndpointPath): the system's ingress routes the manifest's path. Name the same path in both.");
        return routes;
    }

    /// <summary>Composes the gateway on <paramref name="builder"/>: the production graph, with
    /// <paramref name="replaceExternals"/> applied last so a test host can swap the externals.</summary>
    internal WebApplication Build(WebApplicationBuilder builder,
        Action<IServiceCollection>? replaceExternals = null, Action<GatewayOptions>? overrideOptions = null)
    {
        var routes = Declare();
        var manifest = Manifest;

        var services = builder.Services;
        var configuration = builder.Configuration;

        // The service name is the component name, so traces identify the component through the
        // resource's service.name. Strict mode exports only the framework's Intropy.* spans and the
        // component's own; Tracing:Mode in configuration overrides it. Intropy.Telemetry subscribes
        // to no meters: add the framework's.
        services.AddOpenTelemetry(conf =>
        {
            conf.ServiceNamespace = manifest.Organization;
            conf.ServiceName = manifest.ComponentName;
            conf.Environment = builder.Environment.EnvironmentName;
            conf.Mode = TracingMode.Strict;
            conf.ConfigureMetrics = metrics => metrics.AddMeter(IntropyTelemetry.Meters);
        });

        ComponentPlatform.Add(services, configuration, manifest);
        ComponentPublishing.AddPublisher(services, manifest);
        // Before the routes: a step class the component registers itself keeps its registration.
        ConfigureServices(services, configuration);
        foreach (var route in routes)
            route.Register(services);

        var section = configuration.GetSection(GatewayOptions.SectionName);
        services.AddGateway(new GatewayDefinition
        {
            Endpoints = declare =>
            {
                foreach (var route in routes)
                    route.AddTo(declare, IncidentRule.ByBusinessIdOrRequest);
            },
            Respond = Respond,
            ConfigureEndpoints = ConfigureEndpoints,
            ConfigureOptions = options =>
            {
                section.Bind(options);
                ConfigureOptions(options);
                overrideOptions?.Invoke(options);
            },
        });
        replaceExternals?.Invoke(services);

        var app = builder.Build();
        ConfigureApplication(app);
        app.MapGateway();
        return app;
    }
}
