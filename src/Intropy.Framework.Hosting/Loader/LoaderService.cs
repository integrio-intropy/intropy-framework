using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>
/// A loader's process lifetime: serves the Dapr gRPC app callback on
/// <see cref="LoaderOptions.CallbackPort"/>, through which the sidecar pushes the loader's
/// messages, from host start until host stop. A route that cannot be composed stops the host with
/// exit code 1 before the callback listens. On stop, messages in flight get
/// <see cref="LoaderOptions.ShutdownGracePeriod"/> to finish before they are interrupted and left
/// for redelivery; deliveries arriving meanwhile are left for redelivery too. Only then does the
/// callback stop.
/// </summary>
internal sealed class LoaderService(
    MessageConsumer consumer,
    LoaderRouteTable routes,
    LoaderOptions options,
    string componentName,
    IServiceScopeFactory scopes,
    ILoggerFactory loggerFactory,
    IHostApplicationLifetime lifetime) : IHostedService, IAsyncDisposable
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<LoaderService>();
    private MessageCallbackServer? _server;

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

        _server = new MessageCallbackServer(consumer, options.CallbackPort, loggerFactory);
        var port = await _server.StartAsync(cancellationToken);

        _logger.LogInformation(
            "Loader {Component} serving the Dapr app callback on port {Port} for topic {Topic} on {PubSub}; routes: {Routes}; unrouted messages: {Unrouted}",
            componentName, port, options.TopicName, options.PubSubName,
            string.Join(", ", routes.Routes.Select(r => r.Name)), routes.IsRouting ? options.Unrouted.ToString() : "n/a");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_server is null)
            return;

        await consumer.StopAsync();
        await _server.StopAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (_server is not null)
            await _server.DisposeAsync();
        consumer.Dispose();
    }
}
