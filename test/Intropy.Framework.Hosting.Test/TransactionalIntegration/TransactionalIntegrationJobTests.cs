using System.Diagnostics;
using CloudNative.CloudEvents;
using Grpc.Core;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Intropy.Framework.Testing.Adapters;
using Intropy.Framework.Testing.Delivery;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

public class TransactionalIntegrationJobTests
{
    private const string SourceKey = "source";
    private static readonly ContextFactory<Context> NewContext = (metadata, isRetry) => new Context(metadata, isRetry);
    private static readonly FrameworkOptions Identity = new() { ComponentName = "test-integration", ServiceNamespace = "test" };

    private readonly IReceivePipeline<Context> _receivePipeline = Substitute.For<IReceivePipeline<Context>>();
    private readonly ISendPipeline<Context> _sendPipeline = Substitute.For<ISendPipeline<Context>>();

    private readonly TransactionalIntegrationOptions _options = new()
    {
        DaprPubSubName = "test-pubsub",
        DaprTopicName = "test-topic",
        IdleTimeout = TimeSpan.FromSeconds(1),
        PostIdleGracePeriod = TimeSpan.FromSeconds(1),
        MaxMessageProcessingTime = TimeSpan.FromSeconds(30)
    };

    private readonly ILoggerFactory _loggerFactory = Substitute.For<ILoggerFactory>();

    public TransactionalIntegrationJobTests()
    {
        var mockLogger = Substitute.For<ILogger>();
        _loggerFactory.CreateLogger(Arg.Any<string>()).Returns(mockLogger);
    }

    /// <summary>The callback port of the run <see cref="Lifecycle"/> built last.</summary>
    private int _callbackPort;

    [Fact]
    public async Task ExecuteAsync_ShouldPublishEverySourceFile_AndCompleteItOnlyAfterPublishing()
    {
        // Verifies that each file goes through the receive pipeline while it is still in the
        // source, and is deleted only once the pipeline succeeded
        var source = new InMemoryFileAdapter().AddFile("file1.txt", "one").AddFile("file2.txt", "two");
        var presentWhilePublishing = new List<bool>();
        _receivePipeline.Execute(Arg.Any<SourceItem>(), Arg.Any<Context>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var item = callInfo.Arg<SourceItem>();
                presentWhilePublishing.Add(source.Files.ContainsKey(item.Id));
                return (new StepResult<SourceItem>.Success(item), callInfo.Arg<Context>());
            });

        var summary = await Lifecycle(source).ExecuteAsync(CancellationToken.None);
        var firstContent = "one"u8.ToArray();

        Assert.Equal([true, true], presentWhilePublishing);
        Assert.Empty(source.Files);
        Assert.Equal(2, summary.Processed);
        await _receivePipeline.Received(1).Execute(
            Arg.Is<SourceItem>(item => item.Id == "file1.txt" && item.Data.SequenceEqual(firstContent)),
            Arg.Any<Context>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_GivesEachFileAFreshContextNamingTheSourceFile()
    {
        // Verifies that the receive pipeline sees the framework's source file key, the same
        // key extractors get, and that contexts are not shared between files
        var source = new InMemoryFileAdapter().AddFile("file1.txt", "one").AddFile("file2.txt", "two");
        var contexts = new List<Context>();
        _receivePipeline.Execute(Arg.Any<SourceItem>(), Arg.Any<Context>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                contexts.Add(callInfo.Arg<Context>());
                return (new StepResult<SourceItem>.Success(callInfo.Arg<SourceItem>()), callInfo.Arg<Context>());
            });

        await Lifecycle(source).ExecuteAsync(CancellationToken.None);

        Assert.Equal(["file1.txt", "file2.txt"], contexts.Select(c => c.Metadata[SourceContextKeys.FileName]));
        Assert.NotSame(contexts[0], contexts[1]);
    }

    [Fact]
    public async Task ExecuteAsync_RunsEachFileAsItsOwnTrace()
    {
        // Verifies that the receive side traces files the same way extractors do: the receive
        // pipeline continues the file's own trace (whose root the sweep links to the job), so the
        // trace carries on through the queue
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Intropy.Framework.Hosting",
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        var source = new InMemoryFileAdapter().AddFile("file1.txt", "one");
        Activity? current = null;
        _receivePipeline.Execute(Arg.Any<SourceItem>(), Arg.Any<Context>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                current = Activity.Current;
                return (new StepResult<SourceItem>.Success(callInfo.Arg<SourceItem>()), callInfo.Arg<Context>());
            });

        await Lifecycle(source).ExecuteAsync(CancellationToken.None);

        await _receivePipeline.Received(1).Execute(Arg.Any<SourceItem>(), Arg.Any<Context>(), false,
            Arg.Any<CancellationToken>());
        Assert.NotNull(current);
        Assert.StartsWith("process ", current.DisplayName);
        Assert.Equal("file1.txt", current.GetTagItem("file.name"));
    }

    [Fact]
    public async Task ExecuteAsync_GivesTheReceivePipelineTheIntegrationsOwnContext()
    {
        // Verifies that an integration with its own context type gets it from its factory on the
        // receive side, with the framework's source file key set
        var source = new InMemoryFileAdapter().AddFile("file1.txt", "one");
        var receivePipeline = Substitute.For<IReceivePipeline<OrderContext>>();
        OrderContext? received = null;
        receivePipeline.Execute(Arg.Any<SourceItem>(), Arg.Do<OrderContext>(c => received = c), Arg.Any<bool>(),
                Arg.Any<CancellationToken>())
            .Returns(callInfo => (new StepResult<SourceItem>.Success(callInfo.Arg<SourceItem>()), callInfo.Arg<OrderContext>()));
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IFileAdapter>(SourceKey, source);
        services.AddSourcePort(SourceKey);
        services.AddSingleton(Identity);
        services.AddSingleton(receivePipeline);
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        ContextFactory<OrderContext> factory = (metadata, isRetry) => new OrderContext(metadata, isRetry);
        var sendProcessor = new MessageProcessor<OrderContext>(Substitute.For<ISendPipeline<OrderContext>>(), factory,
            "test-integration", Substitute.For<ILogger<MessageProcessor<OrderContext>>>());
        var options = new TransactionalIntegrationOptions
        {
            DaprPubSubName = _options.DaprPubSubName,
            DaprTopicName = _options.DaprTopicName,
            IdleTimeout = _options.IdleTimeout,
            PostIdleGracePeriod = _options.PostIdleGracePeriod,
            CallbackPort = AppCallbackDelivery.AvailablePort()
        };
        var lifecycle = new TransactionalIntegrationJob<OrderContext>(
            new TransactionalIntegrationReceiver<OrderContext>(provider, Identity, factory, _loggerFactory),
            new MessageSubscriber<OrderContext>(sendProcessor, options, "test-integration", _loggerFactory),
            _loggerFactory);

        await lifecycle.ExecuteAsync(CancellationToken.None);

        Assert.NotNull(received);
        Assert.Equal("file1.txt", received.Metadata[SourceContextKeys.FileName]);
        Assert.False(received.IsRetry);
    }

    [Fact]
    public async Task ExecuteAsync_WithoutAReceivePipeline_FailsBeforeTouchingTheSource()
    {
        // Verifies that a misconfigured integration fails the run once, before any file is
        // listed, instead of failing every file
        var source = new InMemoryFileAdapter().AddFile("file1.txt", "one");
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IFileAdapter>(SourceKey, source);
        services.AddSourcePort(SourceKey);
        services.AddSingleton(Identity);
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var receiver = new TransactionalIntegrationReceiver<Context>(provider, Identity, NewContext, _loggerFactory);
        var lifecycle = new TransactionalIntegrationJob<Context>(receiver, Subscriber(_options), _loggerFactory);

        await Assert.ThrowsAsync<InvalidOperationException>(() => lifecycle.ExecuteAsync(CancellationToken.None));

        Assert.True(source.Files.ContainsKey("file1.txt"));
    }

    [Fact]
    public async Task ExecuteAsync_WhenPublishingFails_KeepsTheSourceFileAndReportsTheFailure()
    {
        // Verifies that a file whose content never reached the queue is not completed
        var source = new InMemoryFileAdapter().AddFile("file1.txt", "one");
        _receivePipeline.Execute(Arg.Any<SourceItem>(), Arg.Any<Context>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => (
                new StepResult<SourceItem>.TechnicalFailure(new TechnicalFailure(Description: "Queue unavailable")),
                callInfo.Arg<Context>()));

        var summary = await Lifecycle(source).ExecuteAsync(CancellationToken.None);

        Assert.True(source.Files.ContainsKey("file1.txt"));
        Assert.Equal(1, summary.Failed);
    }

    [Fact]
    public async Task ExecuteAsync_WithArchiveCompletion_ArchivesThePublishedFile()
    {
        // Verifies that the configured completion is applied after publishing
        var source = new InMemoryFileAdapter().AddFile("file1.txt", "one");
        _receivePipeline.Execute(Arg.Any<SourceItem>(), Arg.Any<Context>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => (new StepResult<SourceItem>.Success(callInfo.Arg<SourceItem>()), callInfo.Arg<Context>()));

        await Lifecycle(source, FileCompletion.Archive("archive")).ExecuteAsync(CancellationToken.None);

        Assert.False(source.Files.ContainsKey("file1.txt"));
        Assert.Equal("one", source.GetString("archive", "file1.txt"));
    }

    [Fact]
    public async Task ExecuteAsync_WhenTheHostCancels_StopsAndKeepsTheUnpublishedFile()
    {
        // Verifies that host cancellation ends the run: the interrupted file stays in the
        // source, and the subscriber stops waiting instead of hanging until the idle timeout
        var source = new InMemoryFileAdapter().AddFile("file1.txt", "one");
        using var cts = new CancellationTokenSource();
        _receivePipeline.Execute(Arg.Any<SourceItem>(), Arg.Any<Context>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns<Task<(StepResult<SourceItem>, Context)>>(async callInfo =>
            {
                await cts.CancelAsync();
                await Task.Delay(Timeout.Infinite, callInfo.Arg<CancellationToken>());
                throw new InvalidOperationException("unreachable");
            });

        var summary = await Lifecycle(source).ExecuteAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(source.Files.ContainsKey("file1.txt"));
        Assert.Equal(0, summary.Processed);
        Assert.Equal(0, summary.Failed);
    }

    [Fact]
    public async Task ExecuteAsync_ServesTheCallbackWhileTheRunLasts()
    {
        // The sidecar pushes the integration's queue to the run's callback: it listens while the
        // source is still being swept.
        var (source, sweepMayFinish) = GatedSource();
        var run = Lifecycle(source).ExecuteAsync(CancellationToken.None);

        using var delivery = await ConnectAsync();
        Assert.Empty(await delivery.GetSubscriptionsAsync());
        sweepMayFinish.SetResult();

        await run.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldCompleteSuccessfully_WhenNoSourceItemsExist()
    {
        // Verifies graceful handling when there are no source items to process
        var summary = await Lifecycle(new InMemoryFileAdapter()).ExecuteAsync(CancellationToken.None);

        Assert.Equal(0, summary.Failed);
        await _receivePipeline.DidNotReceiveWithAnyArgs().Execute(default!, default!, default);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldProcessPushedMessages()
    {
        // Verifies that messages pushed to the run's callback are passed to the send pipeline
        var pipelineResult = ((StepResult<string>)new StepResult<string>.Success(""),
            new Context(new Dictionary<string, string>()));
        _sendPipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(pipelineResult));
        var (source, sweepMayFinish) = GatedSource();
        var run = Lifecycle(source).ExecuteAsync(CancellationToken.None);

        using var delivery = await ConnectAsync();
        var ack = await delivery.DeliverAsync(Event("test-msg", "data"));
        sweepMayFinish.SetResult();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryAck.Success, ack);
        await _sendPipeline.Received(1).Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_CountsMessagesLeftForRedeliveryAsFailed_AndTagsTheJobSpan()
    {
        // A run whose deliveries failed must not look clean: each message counts once, by its last
        // outcome in the run, and a message that failed and then succeeded on redelivery is processed
        _sendPipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                // The sidecar hands the payload over as the JSON the publisher wrote: a quoted string.
                var data = System.Text.Encoding.UTF8.GetString(callInfo.Arg<ReadOnlyMemory<byte>>().Span);
                var result = data.Contains("fail", StringComparison.Ordinal)
                    ? (StepResult<string>)new StepResult<string>.TechnicalFailure(new TechnicalFailure("send failed"))
                    : data.Contains("duplicate", StringComparison.Ordinal)
                        ? new StepResult<string>.Cancelled()
                        : new StepResult<string>.Success("");
                return (result, callInfo.Arg<Context>());
            });
        using var jobSource = new ActivitySource($"lifecycle-test-{Guid.NewGuid()}");
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source == jobSource,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        Hosting.Jobs.RunSummary summary;
        using (var job = jobSource.StartActivity("job")!)
        {
            var (source, sweepMayFinish) = GatedSource();
            var run = Lifecycle(source).ExecuteAsync(CancellationToken.None);
            using (var delivery = await ConnectAsync())
            {
                await delivery.DeliverAsync(Event("m1", "fail"));
                await delivery.DeliverAsync(Event("m2", "fail"));
                await delivery.DeliverAsync(Event("m2", "ok"));
                await delivery.DeliverAsync(Event("m3", "duplicate"));
            }
            sweepMayFinish.SetResult();
            summary = await run.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(1, job.GetTagItem("intropy.messages.processed"));
            Assert.Equal(1, job.GetTagItem("intropy.messages.failed"));
            Assert.Equal(1, job.GetTagItem("intropy.messages.skipped"));
        }

        Assert.Equal(0, summary.Processed); // no files
        Assert.Equal(1, summary.Failed);    // m1, left for redelivery
    }

    private static CloudEvent Event(string id, string data) => new()
    {
        Id = id, Source = new Uri("urn:test-source"), Type = "test-type", DataContentType = "application/json",
        Data = data
    };

    /// <summary>A source with no files whose sweep finishes only when the test says so, so the run
    /// keeps serving its callback while the test delivers.</summary>
    private static (IFileAdapter Source, TaskCompletionSource SweepMayFinish) GatedSource()
    {
        var sweepMayFinish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = Substitute.For<IFileAdapter>();
        source.ListAsync().Returns(async _ =>
        {
            await sweepMayFinish.Task;
            return new List<Adapters.Common.FileEntry>();
        });
        return (source, sweepMayFinish);
    }

    /// <summary>A delivery to the callback of the run built last, once it listens.</summary>
    private async Task<AppCallbackDelivery> ConnectAsync()
    {
        var delivery = new AppCallbackDelivery(_callbackPort, _options.DaprPubSubName, _options.DaprTopicName);
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (true)
        {
            try
            {
                await delivery.GetSubscriptionsAsync();
                return delivery;
            }
            catch (RpcException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(20);
            }
        }
    }

    private TransactionalIntegrationJob<Context> Lifecycle(IFileAdapter source, FileCompletion? completion = null)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(SourceKey, source);
        services.AddSourcePort(SourceKey, completion);
        services.AddSingleton(Identity);
        services.AddSingleton(_receivePipeline);
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var options = new TransactionalIntegrationOptions
        {
            DaprPubSubName = _options.DaprPubSubName,
            DaprTopicName = _options.DaprTopicName,
            IdleTimeout = _options.IdleTimeout,
            PostIdleGracePeriod = _options.PostIdleGracePeriod,
            MaxMessageProcessingTime = _options.MaxMessageProcessingTime,
            CallbackPort = _callbackPort = AppCallbackDelivery.AvailablePort()
        };
        var receiver = new TransactionalIntegrationReceiver<Context>(provider, Identity, NewContext, _loggerFactory);
        return new TransactionalIntegrationJob<Context>(receiver, Subscriber(options), _loggerFactory);
    }

    /// <summary>Builds the send side around the shared substitutes, composing the processor the
    /// way the DI registration does.</summary>
    private MessageSubscriber<Context> Subscriber(TransactionalIntegrationOptions options) =>
        new(new MessageProcessor<Context>(_sendPipeline, NewContext, "test-integration",
                Substitute.For<ILogger<MessageProcessor<Context>>>()),
            options, "test-integration", _loggerFactory);

    public sealed record OrderContext(Dictionary<string, string> Metadata, bool IsRetry) : Context(Metadata, IsRetry);
}
