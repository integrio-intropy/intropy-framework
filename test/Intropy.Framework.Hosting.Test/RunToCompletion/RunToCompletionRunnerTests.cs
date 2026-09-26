using System.Diagnostics;
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
        SidecarTimeout = TimeSpan.FromSeconds(1),
        SidecarShutdownTimeout = TimeSpan.FromSeconds(1)
    };

    public RunToCompletionRunnerTests()
    {
        _loggerFactory.CreateLogger(Arg.Any<string>()).Returns(Substitute.For<ILogger>());
    }

    [Fact]
    public async Task RunAsync_ShouldReturnSuccess_WhenJobCompletesWithNoFailures()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .Returns(new JobRunSummary(Processed: 10, Failed: 0, Skipped: 2));

        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.Success, result);
    }

    [Fact]
    public async Task RunAsync_ShouldReturnSuccess_WhenJobHasNothingToDo()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>()).Returns(JobRunSummary.Empty);

        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.Success, result);
    }

    [Fact]
    public async Task RunAsync_ShouldReturnSuccess_WhenCancelledDuringSidecarWait()
    {
        // External cancellation before the job starts is success by design — the
        // job is idempotent and must not be retried by the scheduler. It must not
        // be confused with a sidecar timeout (infrastructure failure).
        _daprClient.WaitForSidecarAsync(Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                var ct = callInfo.Arg<CancellationToken>();
                await Task.Delay(Timeout.Infinite, ct);
            });

        using var cts = new CancellationTokenSource();
        var runner = CreateRunner();

        var run = runner.RunAsync(cts.Token);
        await cts.CancelAsync();

        var result = await run;

        Assert.Equal(RunToCompletionExitCodes.Success, result);
        await _job.DidNotReceive().ExecuteAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_ShouldReturnJobFailure_WhenJobReportsFailedItems()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .Returns(new JobRunSummary(Processed: 10, Failed: 1, Skipped: 0));

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
    public async Task RunAsync_ShouldReturnSuccess_WhenHostCancelsTheJob()
    {
        // Host cancellation is success by design: the job is idempotent.
        using var cts = new CancellationTokenSource();
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .Returns<Task<JobRunSummary>>(async _ =>
            {
                await cts.CancelAsync();
                throw new OperationCanceledException(cts.Token);
            });

        var runner = CreateRunner();

        var result = await runner.RunAsync(cts.Token);

        Assert.Equal(RunToCompletionExitCodes.Success, result);
    }

    [Fact]
    public async Task RunAsync_ShouldReturnJobFailure_WhenJobThrowsCancellationTheHostDidNotRequest()
    {
        // A stray OperationCanceledException (e.g. an HTTP timeout) must not report success.
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());

        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.JobFailure, result);
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
        using var cts = new CancellationTokenSource();
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .Returns<Task<JobRunSummary>>(async _ =>
            {
                await cts.CancelAsync();
                throw new OperationCanceledException(cts.Token);
            });

        var runner = CreateRunner();

        await runner.RunAsync(cts.Token);

        await _daprClient.Received(1).ShutdownSidecarAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_ShouldNotHang_WhenSidecarShutdownIsWedged()
    {
        // A wedged sidecar must not hang the job — the scheduler would record a
        // failed run despite a successful execution.
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .Returns(new JobRunSummary(Processed: 5, Failed: 0, Skipped: 0));
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

    [Fact]
    public async Task RunAsync_MarksTheJobSpanAsAnError_WhenJobReportsFailedItems()
    {
        // The exit code says the run failed; the trace must say so too.
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .Returns(new JobRunSummary(Processed: 10, Failed: 2, Skipped: 0));

        var span = await CaptureJobSpanAsync(CreateRunner());

        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal("2 item(s) failed", span.StatusDescription);
    }

    [Fact]
    public async Task RunAsync_LeavesTheJobSpanUnset_WhenNoItemFailed()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .Returns(new JobRunSummary(Processed: 10, Failed: 0, Skipped: 2));

        var span = await CaptureJobSpanAsync(CreateRunner());

        Assert.Equal(ActivityStatusCode.Unset, span.Status);
    }

    [Fact]
    public async Task RunAsync_RecordsTheExceptionOnTheJobSpan_WhenJobThrows()
    {
        _job.ExecuteAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("job failed"));

        var span = await CaptureJobSpanAsync(CreateRunner());

        Assert.Equal(ActivityStatusCode.Error, span.Status);
        var exception = Assert.Single(span.Events, e => e.Name == "exception");
        Assert.Contains(exception.Tags, t => t.Key == "exception.type" &&
            (string?)t.Value == typeof(InvalidOperationException).FullName);
    }

    /// <summary>Runs the job under a test-owned root span and returns the runner's job span in its
    /// trace only, so spans from tests running in parallel are ignored.</summary>
    private async Task<Activity> CaptureJobSpanAsync(RunToCompletionRunner runner)
    {
        using var testSource = new ActivitySource($"runner-tracing-test-{Guid.NewGuid()}");
        var stopped = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source == testSource || source.Name == "Intropy.Framework.Hosting",
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (stopped) stopped.Add(activity); }
        };
        ActivitySource.AddActivityListener(listener);

        ActivityTraceId traceId;
        using (var root = testSource.StartActivity("test")!)
        {
            traceId = root.TraceId;
            await runner.RunAsync();
        }

        lock (stopped)
            return Assert.Single(stopped, a => a.DisplayName == _options.JobName && a.TraceId == traceId);
    }

    private RunToCompletionRunner CreateRunner() =>
        new(_daprClient, _job, _options, _loggerFactory);
}
