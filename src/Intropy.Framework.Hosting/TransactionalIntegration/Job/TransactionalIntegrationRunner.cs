using Dapr.Client;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Job;

/// <summary>
/// Runs the full transactional integration lifecycle, including sidecar management and tracing.
/// </summary>
public class TransactionalIntegrationRunner
{
    private readonly DaprClient _daprClient;
    private readonly TransactionalIntegrationLifecycle _lifecycle;
    private readonly TransactionalIntegrationOptions _options;
    private readonly ILogger<TransactionalIntegrationRunner> _logger;

    /// <summary>
    /// Creates a new instance of <see cref="TransactionalIntegrationRunner"/>.
    /// </summary>
    /// <param name="daprClient">The Dapr client used for sidecar operations.</param>
    /// <param name="lifecycle">The transactional integration lifecycle to run.</param>
    /// <param name="options">Configuration options.</param>
    /// <param name="loggerFactory">Logger factory for creating loggers.</param>
    public TransactionalIntegrationRunner(
        DaprClient daprClient,
        TransactionalIntegrationLifecycle lifecycle,
        TransactionalIntegrationOptions options,
        ILoggerFactory loggerFactory)
    {
        _daprClient = daprClient;
        _lifecycle = lifecycle;
        _options = options;
        _logger = loggerFactory.CreateLogger<TransactionalIntegrationRunner>();
    }

    /// <summary>
    /// Runs the transactional integration lifecycle.
    /// Waits for the Dapr sidecar, executes the lifecycle within a tracing activity, and shuts down the sidecar.
    /// </summary>
    /// <returns>0 on success, 1 on failure.</returns>
    public async Task<int> RunAsync()
    {
        try
        {
            await DaprSidecarManager.WaitAsync(_daprClient, _options.SidecarTimeoutSeconds, _logger);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Dapr sidecar did not become available within {TimeoutSeconds} seconds",
                _options.SidecarTimeoutSeconds);
            return 1;
        }

        try
        {
            using var activity = ActivitySourceProvider.ActivitySource.StartActivity();
            await _lifecycle.Start();
            return 0;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Transactional integration failed");
            return 1;
        }
        finally
        {
            await DaprSidecarManager.ShutdownAsync(_daprClient, _options.SidecarShutdownTimeoutSeconds, _logger);
        }
    }
}
