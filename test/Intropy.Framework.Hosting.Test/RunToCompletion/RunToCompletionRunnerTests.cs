using Dapr.Client;
using Intropy.Framework.Hosting.RunToCompletion;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Intropy.Framework.Hosting.Test.RunToCompletion;

public class RunToCompletionRunnerTests
{
    private readonly DaprClient _daprClient = Substitute.For<DaprClient>();
    private readonly IRunToCompletionJob _job = Substitute.For<IRunToCompletionJob>();
    private readonly ILoggerFactory _loggerFactory = Substitute.For<ILoggerFactory>();

    private readonly RunToCompletionOptions _options = new()
    {
        JobName = "test-job",
        SidecarTimeoutSeconds = 1,
        SidecarShutdownTimeoutSeconds = 1
    };

    public RunToCompletionRunnerTests()
    {
        _loggerFactory.CreateLogger(Arg.Any<string>()).Returns(Substitute.For<ILogger>());
    }

    [Fact]
    public async Task RunAsync_ShouldReturnSuccess_WhenJobCompletesWithNoFailures()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .Returns(new JobRunSummary(Processed: 10, Failed: 0, Cancelled: 2));

        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.Success, result);
    }

    [Fact]
    public async Task RunAsync_ShouldReturnJobFailure_WhenJobReportsFailedItems()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .Returns(new JobRunSummary(Processed: 10, Failed: 1, Cancelled: 0));

        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.JobFailure, result);
    }

    [Fact]
    public async Task RunAsync_ShouldReturnJobFailure_WhenJobThrows()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("job failed"));

        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.JobFailure, result);
    }

    [Fact]
    public async Task RunAsync_ShouldReturnSuccess_WhenJobIsCancelled()
    {
        // Cancellation is success by design: the job is idempotent and decided it
        // does not need to process (e.g. duplicates detected).
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());

        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.Success, result);
    }

    [Fact]
    public async Task RunAsync_ShouldReturnInfrastructureFailure_WhenSidecarTimesOut()
    {
        _daprClient.WaitForSidecarAsync(Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                var ct = callInfo.Arg<CancellationToken>();
                await Task.Delay(Timeout.Infinite, ct);
            });

        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.InfrastructureFailure, result);
    }

    [Fact]
    public async Task RunAsync_ShouldNotInvokeJob_WhenSidecarTimesOut()
    {
        _daprClient.WaitForSidecarAsync(Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                var ct = callInfo.Arg<CancellationToken>();
                await Task.Delay(Timeout.Infinite, ct);
            });

        var runner = CreateRunner();

        await runner.RunAsync();

        await _job.DidNotReceive().ExecuteAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_ShouldPassCancellationTokenToJob()
    {
        using var cts = new CancellationTokenSource();
        _job.ExecuteAsync(Arg.Any<CancellationToken>()).Returns(JobRunSummary.Empty);

        var runner = CreateRunner();

        await runner.RunAsync(cts.Token);

        await _job.Received(1).ExecuteAsync(cts.Token);
    }

    [Fact]
    public async Task RunAsync_ShouldShutdownSidecar_WhenJobSucceeds()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>()).Returns(JobRunSummary.Empty);

        var runner = CreateRunner();

        await runner.RunAsync();

        await _daprClient.Received(1).ShutdownSidecarAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_ShouldShutdownSidecar_WhenJobThrows()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("job failed"));

        var runner = CreateRunner();

        await runner.RunAsync();

        await _daprClient.Received(1).ShutdownSidecarAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_ShouldShutdownSidecar_WhenJobIsCancelled()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());

        var runner = CreateRunner();

        await runner.RunAsync();

        await _daprClient.Received(1).ShutdownSidecarAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_ShouldNotHang_WhenSidecarShutdownIsWedged()
    {
        // A wedged sidecar must not hang the job — the scheduler would record a
        // failed run despite a successful execution.
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .Returns(new JobRunSummary(Processed: 5, Failed: 0, Cancelled: 0));
        _daprClient.ShutdownSidecarAsync(Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                var ct = callInfo.Arg<CancellationToken>();
                await Task.Delay(Timeout.Infinite, ct);
            });

        var runner = CreateRunner();

        var run = runner.RunAsync();
        var completed = await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(10)));

        Assert.Same(run, completed);
        Assert.Equal(RunToCompletionExitCodes.Success, await run);
    }

    [Fact]
    public async Task RunAsync_ShouldPreserveJobOutcome_WhenSidecarShutdownThrows()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>()).Returns(JobRunSummary.Empty);
        _daprClient.ShutdownSidecarAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("shutdown failed"));

        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.Success, result);
    }

    private RunToCompletionRunner CreateRunner() =>
        new(_daprClient, _job, _options, _loggerFactory);
}
