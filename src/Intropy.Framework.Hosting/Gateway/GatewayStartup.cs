using Dapr.Client;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// Starts a gateway: composes each endpoint's pipeline once, so a misconfigured component fails at
/// startup rather than on its first request, then waits for the sidecar in the background and marks the
/// gateway ready. The wait never gives up — a resident gateway stays unready (its readiness probe
/// answers <c>503</c>) until the sidecar answers, instead of exiting into a restart loop.
/// </summary>
internal sealed class GatewayStartup(
    IServiceProvider provider,
    GatewayEndpointSet endpoints,
    GatewayReadiness readiness,
    GatewayOptions options,
    ILogger<GatewayStartup> logger) : BackgroundService
{
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await using (var scope = provider.CreateAsyncScope())
            foreach (var endpoint in endpoints.All)
                endpoint.Validate(scope.ServiceProvider);
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.WaitForSidecar)
        {
            readiness.MarkReady();
            return;
        }

        var dapr = provider.GetRequiredService<DaprClient>();
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DaprSidecarManager.WaitAsync(dapr, options.SidecarTimeout, logger, stoppingToken);
                readiness.MarkReady();
                foreach (var endpoint in endpoints.All)
                    logger.LogInformation("Gateway {ComponentName} is ready on {Path} ({Port})",
                        endpoint.ComponentName, endpoint.Path, endpoint.Port);
                return;
            }
            catch (TimeoutException e)
            {
                logger.LogWarning(e, "Gateway {ComponentName} is waiting for its Dapr sidecar",
                    endpoints.All[0].ComponentName);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
        }
    }
}
