using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Components;
using Intropy.Framework.Components.Gateway;
using Intropy.Framework.Hosting.Gateway;
using Intropy.Framework.Testing.Services;
using Intropy.Framework.Testing.Topics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Intropy.Framework.Testing.Components;

/// <summary>
/// A gateway component served in memory: its production graph — web host, endpoint, steps, rules,
/// pipeline — with its three externals faked: the publisher and the two platform-service clients.
/// A test posts to <see cref="Client"/> and asserts on the fakes, so it exercises exactly what the
/// system host runs.
/// </summary>
/// <remarks>
/// The fakes replace the production registrations rather than adding after them, so they never
/// depend on registration order. The gateway does not wait for a sidecar: it is ready as soon as it
/// starts. Call <see cref="StartAsync"/> (or <see cref="PostAsync"/>, which starts it) once
/// <see cref="Topic"/> and the clients are set up.
/// </remarks>
/// <typeparam name="TComponent">The gateway component.</typeparam>
public sealed class GatewayTestHost<TComponent> : IAsyncDisposable
    where TComponent : GatewayComponent, new()
{
    private readonly string _environment;
    private readonly Action<IServiceCollection>? _configureServices;
    private WebApplication? _app;
    private HttpClient? _client;

    /// <summary>Composes the gateway with its externals faked.</summary>
    /// <param name="environment">The environment whose appsettings overlay applies.</param>
    /// <param name="configureServices">Adjusts the services after the fakes are in place — to swap
    /// one more dependency.</param>
    public GatewayTestHost(string environment = "Development", Action<IServiceCollection>? configureServices = null)
    {
        Endpoints = new TComponent().DeclareEndpoints();
        _environment = environment;
        _configureServices = configureServices;
    }

    /// <summary>The endpoints the gateway declares, checked against the endpoint the component's
    /// manifest names.</summary>
    public IReadOnlyList<GatewayEndpointDeclaration> Endpoints { get; }

    /// <summary>The path of the gateway's one endpoint.</summary>
    /// <exception cref="InvalidOperationException">The gateway declares several endpoints: use
    /// <see cref="PathOf"/>.</exception>
    public string Path => Endpoints.Count == 1
        ? Endpoints[0].Path
        : throw new InvalidOperationException(
            $"{typeof(TComponent).Name} declares {Endpoints.Count} endpoints; name one with PathOf(port).");

    /// <summary>The path of the endpoint whose port is <paramref name="port"/>.</summary>
    /// <param name="port">The endpoint's port.</param>
    /// <returns>The endpoint's path.</returns>
    /// <exception cref="ArgumentException">The gateway declares no such endpoint.</exception>
    public string PathOf(string port) =>
        Endpoints.FirstOrDefault(e => string.Equals(e.Port, port, StringComparison.Ordinal))?.Path
        ?? throw new ArgumentException(
            $"{typeof(TComponent).Name} declares no endpoint '{port}' (it declares " +
            $"{string.Join(", ", Endpoints.Select(e => $"'{e.Port}'"))}).", nameof(port));

    /// <summary>The events the gateway published.</summary>
    public FakeTopic<Context> Topic { get; } = new();

    /// <summary>The Idempotency Service.</summary>
    public FakeIdempotencyServiceClient Idempotency { get; } = new();

    /// <summary>The Business Incident Service.</summary>
    public FakeBusinessIncidentServiceClient Incidents { get; } = new();

    /// <summary>A client of the in-memory gateway. Available after <see cref="StartAsync"/>.</summary>
    public HttpClient Client => _client ?? throw new InvalidOperationException(
        $"The gateway is not started: call {nameof(StartAsync)} first.");

    /// <summary>The started gateway's services. Available after <see cref="StartAsync"/>.</summary>
    public IServiceProvider Services => _app?.Services ?? throw new InvalidOperationException(
        $"The gateway is not started: call {nameof(StartAsync)} first.");

    /// <summary>Composes and starts the gateway, once. Startup composes the pipeline, so a
    /// misconfigured component fails here.</summary>
    /// <returns>A client of the gateway.</returns>
    public async Task<HttpClient> StartAsync()
    {
        if (_client is not null)
            return _client;

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            EnvironmentName = _environment,
            ContentRootPath = AppContext.BaseDirectory,
        });
        builder.WebHost.UseTestServer();

        _app = new TComponent().Build(builder, ReplaceExternals, options => options.WaitForSidecar = false);
        await _app.StartAsync();
        _client = _app.GetTestClient();
        return _client;
    }

    /// <summary>Posts <paramref name="body"/> to an endpoint, starting the gateway if it is not
    /// started.</summary>
    /// <param name="body">The request body.</param>
    /// <param name="contentType">The body's media type.</param>
    /// <param name="requestId">An <c>X-Request-Id</c> to send, or null to let the gateway assign one.</param>
    /// <param name="endpoint">The port of the endpoint to post to; null for the gateway's one
    /// endpoint.</param>
    /// <returns>The gateway's response.</returns>
    public async Task<HttpResponseMessage> PostAsync(string body, string contentType = "application/json",
        string? requestId = null, string? endpoint = null)
    {
        var path = endpoint is null ? Path : PathOf(endpoint);
        var client = await StartAsync();
        using var request = new HttpRequestMessage(HttpMethod.Post, path);
        request.Content = new StringContent(body, System.Text.Encoding.UTF8, contentType);
        if (requestId is not null)
            request.Headers.Add("X-Request-Id", requestId);
        return await client.SendAsync(request);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        _client?.Dispose();
        if (_app is not null)
            await _app.DisposeAsync();
    }

    private void ReplaceExternals(IServiceCollection services)
    {
        services.RemoveAll<IIdempotencyServiceClient>();
        services.AddSingleton<IIdempotencyServiceClient>(Idempotency);
        services.RemoveAll<IBusinessIncidentServiceClient>();
        services.AddSingleton<IBusinessIncidentServiceClient>(Incidents);
        services.RemoveAll<SendStep<Context>>();
        services.AddSingleton<SendStep<Context>>(Topic);
        _configureServices?.Invoke(services);
    }
}
