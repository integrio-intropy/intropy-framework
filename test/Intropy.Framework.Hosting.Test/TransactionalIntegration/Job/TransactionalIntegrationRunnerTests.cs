using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.TransactionalIntegration.Job;
using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Job;

public class TransactionalIntegrationRunnerTests
{
    private readonly DaprClient _daprClient = Substitute.For<DaprClient>();
    private readonly ILoggerFactory _loggerFactory = Substitute.For<ILoggerFactory>();

    private readonly TransactionalIntegrationOptions _options = new()
    {
        DaprPubSubName = "test-pubsub",
        DaprTopicName = "test-topic",
        SidecarTimeoutSeconds = 1,
        SidecarShutdownTimeoutSeconds = 1
    };

    private readonly StubLifecycle _lifecycle = new();

    public TransactionalIntegrationRunnerTests()
    {
        _loggerFactory.CreateLogger(Arg.Any<string>()).Returns(Substitute.For<ILogger>());
    }

    [Fact]
    public async Task RunAsync_ShouldReturnZero_WhenLifecycleCompletesSuccessfully()
    {
        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(0, result);
    }

    [Fact]
    public async Task RunAsync_ShouldReturnOne_WhenSidecarTimesOut()
    {
        _daprClient.WaitForSidecarAsync(Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                var ct = callInfo.Arg<CancellationToken>();
                await Task.Delay(Timeout.Infinite, ct);
            });

        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(1, result);
    }

    [Fact]
    public async Task RunAsync_ShouldReturnOne_WhenLifecycleThrows()
    {
        _lifecycle.ThrowOnStart = new InvalidOperationException("lifecycle failed");
        var runner = CreateRunner();

        var result = await runner.RunAsync();

        Assert.Equal(1, result);
    }

    [Fact]
    public async Task RunAsync_ShouldShutdownSidecar_WhenLifecycleSucceeds()
    {
        var runner = CreateRunner();

        await runner.RunAsync();

        await _daprClient.Received(1).ShutdownSidecarAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_ShouldShutdownSidecar_WhenLifecycleThrows()
    {
        _lifecycle.ThrowOnStart = new InvalidOperationException("lifecycle failed");

        var runner = CreateRunner();

        await runner.RunAsync();

        await _daprClient.Received(1).ShutdownSidecarAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_ShouldNotHang_WhenSidecarShutdownIsWedged()
    {
        // A wedged sidecar must not hang the host after the lifecycle has completed.
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
        Assert.Equal(0, await run);
    }

    private TransactionalIntegrationRunner CreateRunner() =>
        new(_daprClient, _lifecycle, _options, _loggerFactory);

    private sealed class StubLifecycle : TransactionalIntegrationLifecycle
    {
        public Exception? ThrowOnStart { get; set; }

        public StubLifecycle()
            : base(
                Substitute.For<ISourceLister>(),
                Substitute.For<IReceivePipeline<Context>>(),
                Substitute.For<ITopicSubscriber>(),
                Substitute.For<ISendPipeline<Context>>(),
                new TransactionalIntegrationOptions { DaprPubSubName = "test", DaprTopicName = "test" },
                Substitute.For<ILoggerFactory>())
        {
        }

        public override Task Start()
        {
            if (ThrowOnStart is not null)
                throw ThrowOnStart;
            return Task.CompletedTask;
        }
    }
}
