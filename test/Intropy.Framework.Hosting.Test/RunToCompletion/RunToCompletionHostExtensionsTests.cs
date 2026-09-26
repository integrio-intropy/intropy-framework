using Dapr.Client;
using Intropy.Framework.Hosting.RunToCompletion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.RunToCompletion;

public class RunToCompletionHostExtensionsTests
{
    [Fact]
    public async Task RunToCompletionAsync_StartsTheHostBeforeTheJobRuns()
    {
        var probe = new TelemetryProviderProbe();
        // OpenTelemetry's hosting integration creates its providers in a hosted service; a job
        // that runs before the host starts is never traced.
        var job = new ProbeJob(probe, _ => Task.FromResult(JobRunSummary.Empty));

        await BuildHost(job, probe).RunToCompletionAsync();

        Assert.True(job.HostWasStartedWhenJobRan);
    }

    [Fact]
    public async Task RunToCompletionAsync_DisposesTheHostAfterTheJob_SoTelemetryIsFlushed()
    {
        var probe = new TelemetryProviderProbe();
        var job = new ProbeJob(probe, _ => Task.FromResult(JobRunSummary.Empty));

        await BuildHost(job, probe).RunToCompletionAsync();

        Assert.True(probe.Stopped);
        Assert.True(probe.Disposed);
    }

    [Fact]
    public async Task RunToCompletionAsync_ReturnsTheRunnersExitCode()
    {
        var probe = new TelemetryProviderProbe();
        var job = new ProbeJob(probe, _ => Task.FromResult(new JobRunSummary(Processed: 1, Failed: 1, Skipped: 0)));

        var exitCode = await BuildHost(job, probe).RunToCompletionAsync();

        Assert.Equal(RunToCompletionExitCodes.JobFailure, exitCode);
        Assert.True(probe.Disposed);
    }

    [Fact]
    public async Task RunToCompletionAsync_CancelsTheJob_WhenTheHostIsAskedToStop()
    {
        var probe = new TelemetryProviderProbe();
        // SIGTERM reaches the job through the host's lifetime; host cancellation is a success.
        IHost? host = null;
        var job = new ProbeJob(probe, async ct =>
        {
            host!.Services.GetRequiredService<IHostApplicationLifetime>().StopApplication();
            await Task.Delay(Timeout.Infinite, ct);
            return JobRunSummary.Empty;
        });
        host = BuildHost(job, probe);

        var run = host.RunToCompletionAsync();
        var completed = await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(10)));

        Assert.Same(run, completed);
        Assert.Equal(RunToCompletionExitCodes.Success, await run);
        Assert.True(probe.Disposed);
    }

    [Fact]
    public async Task RunToCompletionAsync_ReturnsInfrastructureFailure_WhenTheHostFailsToStart()
    {
        var probe = new TelemetryProviderProbe();
        var job = new ProbeJob(probe, _ => Task.FromResult(JobRunSummary.Empty));
        probe.FailOnStart = true;

        var exitCode = await BuildHost(job, probe).RunToCompletionAsync();

        Assert.Equal(RunToCompletionExitCodes.InfrastructureFailure, exitCode);
        Assert.False(job.Ran);
        Assert.True(probe.Disposed);
    }

    private static IHost BuildHost(ProbeJob job, TelemetryProviderProbe probe) =>
        new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(Substitute.For<DaprClient>());
                // Stands in for OpenTelemetry's providers: created by a hosted service, flushed on disposal.
                services.AddSingleton(probe);
                services.AddHostedService(sp => sp.GetRequiredService<TelemetryProviderProbe>());
                services.AddSingleton(job);
                services.AddRunToCompletionJob<ProbeJob>(options =>
                {
                    options.JobName = "host-test-job";
                    options.SidecarTimeout = TimeSpan.FromSeconds(1);
                    options.SidecarShutdownTimeout = TimeSpan.FromSeconds(1);
                });
            })
            .Build();

    private sealed class TelemetryProviderProbe : IHostedService, IAsyncDisposable
    {
        public bool FailOnStart { get; set; }
        public bool Started { get; private set; }
        public bool Stopped { get; private set; }
        public bool Disposed { get; private set; }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (FailOnStart)
                throw new InvalidOperationException("exporter misconfigured");
            Started = true;
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Stopped = true;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ProbeJob(TelemetryProviderProbe probe, Func<CancellationToken, Task<JobRunSummary>> execute)
        : IRunToCompletionJob
    {
        public bool Ran { get; private set; }
        public bool HostWasStartedWhenJobRan { get; private set; }

        public Task<JobRunSummary> ExecuteAsync(CancellationToken ct)
        {
            Ran = true;
            HostWasStartedWhenJobRan = probe.Started;
            return execute(ct);
        }
    }
}
