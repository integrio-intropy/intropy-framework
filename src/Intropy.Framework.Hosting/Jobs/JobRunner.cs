using System.Diagnostics;
using Dapr.Client;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Jobs;

/// <summary>
/// Hosts an <see cref="IJob"/>: waits for the Dapr sidecar, executes the job
/// inside a traced activity, shuts the sidecar down, and maps the outcome to an exit code
/// (see <see cref="JobExitCodes"/>).
/// </summary>
public class JobRunner
{
    private readonly DaprClient _daprClient;
    private readonly IJob _job;
    private readonly JobOptions _options;
    private readonly ILogger<JobRunner> _logger;

    /// <summary>
    /// Creates a new instance of <see cref="JobRunner"/>.
    /// </summary>
    /// <param name="daprClient">The Dapr client used for sidecar operations.</param>
    /// <param name="job">The run-to-completion job to execute.</param>
    /// <param name="options">Configuration options.</param>
    /// <param name="loggerFactory">Logger factory for creating loggers.</param>
    public JobRunner(
        DaprClient daprClient,
        IJob job,
        JobOptions options,
        ILoggerFactory loggerFactory)
    {
        _daprClient = daprClient;
        _job = job;
        _options = options;
        _logger = loggerFactory.CreateLogger<JobRunner>();
    }

    /// <summary>
    /// Runs the job to completion.
    /// </summary>
    /// <param name="ct">
    /// A cancellation token owned by the host's entry point (e.g. wired to SIGTERM).
    /// Host cancellation maps to a success exit code by design: jobs are idempotent. An
    /// OperationCanceledException the host did not request is a job failure.
    /// </param>
    /// <returns>
    /// 0 on success or host cancellation, 1 on job failure, 2 on infrastructure failure.
    /// See <see cref="JobExitCodes"/>.
    /// </returns>
    public async Task<int> RunAsync(CancellationToken ct = default)
    {
        var start = Stopwatch.GetTimestamp();
        var exitCode = await RunCoreAsync(ct);
        HostingMetrics.RecordJobRun(_options.JobName, exitCode, Stopwatch.GetElapsedTime(start));
        return exitCode;
    }

    private async Task<int> RunCoreAsync(CancellationToken ct)
    {
        try
        {
            await DaprSidecarManager.WaitAsync(_daprClient, _options.SidecarTimeout, _logger, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _logger.LogInformation("Job {JobName} was cancelled before it started", _options.JobName);
            return JobExitCodes.Success;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Dapr sidecar did not become available within {Timeout} seconds",
                _options.SidecarTimeout.TotalSeconds);
            return JobExitCodes.InfrastructureFailure;
        }

        using var activity = ActivitySourceProvider.ActivitySource.StartActivity(_options.JobName);

        try
        {
            var summary = await _job.ExecuteAsync(ct);

            _logger.LogInformation(
                "Job {JobName} completed: {Processed} processed, {Failed} failed, {Skipped} skipped",
                _options.JobName, summary.Processed, summary.Failed, summary.Skipped);

            activity?.SetTag("intropy.job.processed", summary.Processed);
            activity?.SetTag("intropy.job.failed", summary.Failed);
            activity?.SetTag("intropy.job.skipped", summary.Skipped);

            if (summary.Failed == 0)
                return JobExitCodes.Success;

            // Failed items exit 1 without an exception; the span must agree with the exit code.
            activity?.SetStatus(ActivityStatusCode.Error, $"{summary.Failed} item(s) failed");
            return JobExitCodes.JobFailure;
        }
        catch (InfrastructureUnavailableException e)
        {
            _logger.LogError(e, "Job {JobName} could not run: {Reason}", _options.JobName, e.Message);
            activity?.AddException(e);
            activity?.SetStatus(ActivityStatusCode.Error, e.Message);
            return JobExitCodes.InfrastructureFailure;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Host cancellation is not failure by design: the job is idempotent. A cancellation
            // the host did not request falls through to the job-failure catch below, so a stray
            // OperationCanceledException can never report a failed run as success.
            _logger.LogInformation("Job {JobName} was cancelled", _options.JobName);
            return JobExitCodes.Success;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Job {JobName} failed", _options.JobName);
            activity?.AddException(e);
            activity?.SetStatus(ActivityStatusCode.Error, e.Message);
            return JobExitCodes.JobFailure;
        }
        finally
        {
            await DaprSidecarManager.ShutdownAsync(_daprClient, _options.SidecarShutdownTimeout, _logger);
        }
    }
}
