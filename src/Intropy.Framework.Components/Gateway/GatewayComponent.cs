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
/// as a CloudEvent through the extractor pipeline. It is an extractor whose sources are its
/// callers: endpoints take the place of source ports, and each endpoint's declaration carries the
/// pipeline its items go through. The component declares its endpoints, what each accepts with its
/// steps and rules, and the framework wires the web host, the probes, telemetry, the platform
/// clients and the publisher.
/// </summary>
/// <remarks>
/// <code>
/// [IntropyComponent("order-gateway", Organization = "Acme", Publishes = "order-placed",
///     Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
///     EventSource = "urn:Acme:order-gateway")]
/// public sealed class OrderGateway : GatewayComponent
/// {
///     protected override void Endpoints(GatewayEndpoints endpoints) =&gt;
///         endpoints.Endpoint(endpoint =&gt; endpoint
///             .Accepts&lt;SourceOrder, OrderPlaced&gt;(route =&gt; route
///                 .Steps&lt;OrderDeserializer, OrderValidator, OrderTransformer&gt;()
///                 .Idempotency(key: (o, _) =&gt; o.OrderId, at: (o, _) =&gt; o.OrderDate)
///                 .OutputEvent(subject: o =&gt; o.OrderId, at: o =&gt; o.OrderDate)));
/// }
/// </code>
/// <para>
/// The component's <see cref="IntropyComponentAttribute"/> names the endpoint the system declares
/// (<see cref="IntropyComponentAttribute.Port"/>, <see cref="IntropyComponentAttribute.EndpointPath"/>):
/// startup fails unless the component declares that endpoint with that path, so the identity and
/// the declared endpoints cannot drift apart. <see cref="GatewayEndpoints.Endpoint(Action{GatewayEndpoint})"/>
/// declares exactly that endpoint; further endpoints are the component's own, each named with its
/// port and path.
/// </para>
/// <para>
/// Every other default is a member to override: <see cref="Respond"/>,
/// <see cref="ConfigureEndpoints"/> (authorization, rate limiting),
/// <see cref="ConfigureApplication"/> (middleware), <see cref="ConfigureOptions"/> (the
/// <see cref="GatewayOptions"/> under <c>Gateway</c> in configuration), and each route's pipeline
/// (<see cref="PublishingRoute{TInput,TOutput}.Extend"/>).
/// </para>
/// </remarks>
public abstract class GatewayComponent : IntropyComponent
{
    /// <summary>The port the host listens on when configuration names no URL or port.</summary>
    public const int DefaultHttpPort = 8080;

    private ComponentManifest? _manifest;

    /// <summary>The component's identity, read from its <see cref="IntropyComponentAttribute"/>. Available from
    /// <see cref="Endpoints"/> on.</summary>
    protected ComponentManifest Manifest => _manifest ?? throw new InvalidOperationException(
        $"{GetType().Name}.Manifest is read before the component is composed.");

    /// <summary>Declares the endpoints the gateway exposes, and what each accepts.</summary>
    /// <param name="endpoints">The endpoints.</param>
    protected abstract void Endpoints(GatewayEndpoints endpoints);

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

    /// <inheritdoc />
    internal override async Task<int> RunAsync(string[] args, ComponentManifest manifest)
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

        await using var app = Build(builder, manifest);
        await app.RunAsync();
        return 0;
    }

    /// <summary>The endpoints the component declares for <paramref name="manifest"/>, checked
    /// against the endpoint the component's <see cref="IntropyComponentAttribute"/> names.</summary>
    /// <exception cref="InvalidOperationException">The component does not declare the attributed
    /// endpoint, or declares it at another path.</exception>
    internal IReadOnlyList<GatewayEndpointDeclaration> DeclareEndpoints(ComponentManifest manifest) =>
        Declare(manifest).Declared;

    private GatewayEndpoints Declare(ComponentManifest manifest)
    {
        _manifest = manifest;
        var endpoints = new GatewayEndpoints(() => new GatewayEndpointDeclaration(
            manifest.Require(manifest.Port, "Port"),
            manifest.EndpointPath ?? $"/{manifest.Require(manifest.Publishes, "publishes")}"));
        Endpoints(endpoints);
        var declared = endpoints.Declared;
        if (manifest.Port is not { } attributed)
            return endpoints;

        var endpoint = declared.FirstOrDefault(e => string.Equals(e.Port, attributed, StringComparison.Ordinal))
                       ?? throw new InvalidOperationException(
                           $"{GetType().Name} declares no endpoint '{attributed}', which its IntropyComponent attribute names as the " +
                           $"gateway's port (it declares {string.Join(", ", declared.Select(e => $"'{e.Port}'"))}): " +
                           "the system declares the attributed endpoint, so the component must expose it. Name the same " +
                           "port on the attribute and in Endpoints.");
        if (manifest.EndpointPath is { } path && !string.Equals(endpoint.Path, path, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{GetType().Name} exposes endpoint '{attributed}' at '{endpoint.Path}', but its IntropyComponent attribute names " +
                $"'{path}' (EndpointPath): the system's ingress routes the attributed path. Name the same path in both.");
        return endpoints;
    }

    /// <summary>Composes the gateway on <paramref name="builder"/>: the production graph, with
    /// <paramref name="replaceExternals"/> applied last so a test host can swap the externals.</summary>
    internal WebApplication Build(WebApplicationBuilder builder, ComponentManifest manifest,
        Action<IServiceCollection>? replaceExternals = null, Action<GatewayOptions>? overrideOptions = null)
    {
        var endpoints = Declare(manifest);

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
        // Before the routes: a step slot the component registers here wins over the routes' declared
        // classes.
        ConfigureServices(services, configuration);
        foreach (var route in endpoints.Routes)
            route.Register(services, manifest);

        var section = configuration.GetSection(GatewayOptions.SectionName);
        services.AddGateway(new GatewayDefinition
        {
            Endpoints = declare =>
            {
                foreach (var route in endpoints.Routes)
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
