using System.Globalization;
using Intropy.Framework.Hosting.Loader;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging.Callback;

/// <summary>
/// A loader's process lifetime under <see cref="LoaderTransport.AppCallback"/>: serves the Dapr gRPC
/// app callback on <see cref="LoaderOptions.CallbackPort"/>, through which the sidecar delivers the
/// loader's messages. The component stays a generic host; this runs its own small Kestrel server
/// inside it. A route that cannot be composed stops the host with exit code 1 before the server
/// listens.
/// </summary>
internal sealed class LoaderCallbackServer(
    LoaderCallbackDelivery delivery,
    LoaderRouteTable routes,
    LoaderOptions options,
    string componentName,
    IServiceScopeFactory scopes,
    ILoggerFactory loggerFactory,
    IHostApplicationLifetime lifetime) : IHostedService, IAsyncDisposable
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<LoaderCallbackServer>();
    private WebApplication? _server;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            routes.Verify(scopes, componentName);
        }
        catch (Exception e)
        {
            _logger.LogCritical(e, "Loader {Component} cannot compose its routes; stopping the host", componentName);
            Environment.ExitCode = 1;
            lifetime.StopApplication();
            return;
        }

        var port = Port();
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            ContentRootPath = AppContext.BaseDirectory
        });
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.ListenAnyIP(port, listen => listen.Protocols = HttpProtocols.Http2));
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(loggerFactory);
        builder.Services.AddSingleton(delivery);
        builder.Services.AddGrpc();

        _server = builder.Build();
        _server.MapGrpcService<LoaderAppCallbackService>();
        await _server.StartAsync(cancellationToken);

        _logger.LogInformation(
            "Loader {Component} serving the Dapr app callback on port {Port} for topic {Topic} on {PubSub}; routes: {Routes}; unrouted messages: {Unrouted}",
            componentName, port, options.TopicName, options.PubSubName,
            string.Join(", ", routes.Routes.Select(r => r.Name)), routes.IsRouting ? options.Unrouted.ToString() : "n/a");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_server is not null)
            await _server.StopAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (_server is not null)
            await _server.DisposeAsync();
    }

    private int Port() =>
        options.CallbackPort
        ?? (int.TryParse(Environment.GetEnvironmentVariable("APP_PORT"), NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var appPort) ? appPort : 8080);
}
