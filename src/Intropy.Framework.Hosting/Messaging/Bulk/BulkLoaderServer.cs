using System.Globalization;
using Intropy.Framework.Hosting.Loader;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging.Bulk;

/// <summary>
/// A batch loader's process lifetime: serves the Dapr gRPC app callback on
/// <see cref="LoaderOptions.CallbackPort"/>, through which the sidecar discovers the loader's bulk
/// subscription and delivers its batches. The component stays a generic host; this runs its own
/// small Kestrel server inside it. On stop, batches in flight get
/// <see cref="LoaderOptions.ShutdownGracePeriod"/> to finish before they are interrupted and their
/// entries left for redelivery. A route that cannot be composed stops the host with exit code 1.
/// </summary>
internal sealed class BulkLoaderServer(
    LoaderBulkDelivery delivery,
    LoaderRouteTable routes,
    LoaderOptions options,
    string componentName,
    IServiceScopeFactory scopes,
    ILoggerFactory loggerFactory,
    IHostApplicationLifetime lifetime) : IHostedService, IAsyncDisposable
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<BulkLoaderServer>();
    private WebApplication? _server;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            using (var scope = scopes.CreateScope())
                foreach (var route in routes.Routes)
                    route.Verify(scope.ServiceProvider, componentName);
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
            "Loader {Component} serving the Dapr app callback on port {Port} for topic {Topic} on {PubSub} (bulk, up to {MaxBatchSize} per batch); routes: {Routes}; unrouted messages: {Unrouted}",
            componentName, port, options.TopicName, options.PubSubName, options.MaxBatchSize,
            string.Join(", ", routes.Routes.Select(r => r.Name)), routes.IsRouting ? options.Unrouted.ToString() : "n/a");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_server is null)
            return;

        // Kestrel stops taking new batches and waits for those in flight; when the grace period ends,
        // they are interrupted so their entries come back as Retry before the server goes away.
        using var grace = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        grace.CancelAfter(options.ShutdownGracePeriod);
        await using var interruptOnGraceEnd = grace.Token.Register(delivery.Interrupt);
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
