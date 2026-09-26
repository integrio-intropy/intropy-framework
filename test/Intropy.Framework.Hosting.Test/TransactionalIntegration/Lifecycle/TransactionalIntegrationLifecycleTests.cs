using System.Diagnostics;
using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Sweep;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Intropy.Framework.Hosting.TransactionalIntegration.Lifecycle;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Lifecycle;

public class TransactionalIntegrationLifecycleTests
{
    private const string SourceKey = "source";
    private static readonly ContextFactory<Context> NewContext = (metadata, isRetry) => new Context(metadata, isRetry);
    private static readonly FrameworkOptions Identity = new() { ComponentName = "test-integration", ServiceNamespace = "test" };

    private readonly IReceivePipeline<Context> _receivePipeline = Substitute.For<IReceivePipeline<Context>>();
    private readonly ITopicSubscriber _topicSubscriber = Substitute.For<ITopicSubscriber>();
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

    public TransactionalIntegrationLifecycleTests()
    {
        var mockLogger = Substitute.For<ILogger>();
        _loggerFactory.CreateLogger(Arg.Any<string>()).Returns(mockLogger);

        var subscriptionMock = Substitute.For<IAsyncDisposable>();
        _topicSubscriber.SubscribeAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<TopicMessageHandler>(),
                Arg.Any<CancellationToken>())
            .Returns(subscriptionMock);
    }

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
        Assert.Equal("file1.txt", current.GetTagItem("intropy.file.name"));
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
        var lifecycle = new TransactionalIntegrationLifecycle<OrderContext>(
            new TransactionalIntegrationReceiveJob<OrderContext>(provider, Identity, factory, _loggerFactory),
            _topicSubscriber, Substitute.For<ISendPipeline<OrderContext>>(), _options, "test-integration", factory,
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
        var receiveJob = new TransactionalIntegrationReceiveJob<Context>(provider, Identity, NewContext, _loggerFactory);
        var lifecycle = new TransactionalIntegrationLifecycle<Context>(receiveJob, _topicSubscriber, _sendPipeline, _options,
            "test-integration", NewContext, _loggerFactory);

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

        await Lifecycle(source, SweepCompletion.Archive("archive")).ExecuteAsync(CancellationToken.None);

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
    public async Task ExecuteAsync_ShouldSubscribeToMessages()
    {
        // Verifies that the lifecycle subscribes to the configured topic for message processing
        await Lifecycle(new InMemoryFileAdapter()).ExecuteAsync(CancellationToken.None);

        await _topicSubscriber.Received(1).SubscribeAsync(
            "test-pubsub",
            "test-topic",
            Arg.Any<TimeSpan>(),
            Arg.Any<TopicMessageHandler>(),
            Arg.Any<CancellationToken>());
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
    public async Task ExecuteAsync_ShouldProcessReceivedMessages()
    {
        // Verifies that messages received from the queue are passed to the message handler
        TopicMessageHandler? capturedHandler = null;
        var subscriptionMock = Substitute.For<IAsyncDisposable>();

        _topicSubscriber.SubscribeAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Do((Action<TopicMessageHandler>)(h => capturedHandler = h)),
                Arg.Any<CancellationToken>())
            .Returns(subscriptionMock);

        var pipelineResult = ((StepResult<string>)new StepResult<string>.Success(""),
            new Context(new Dictionary<string, string>()));
        _sendPipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(Task.FromResult(pipelineResult));

        var lifecycle = Lifecycle(new InMemoryFileAdapter());

        _ = Task.Run(async () => await lifecycle.ExecuteAsync(CancellationToken.None));

        await Task.Delay(100);

        Assert.NotNull(capturedHandler);

        var testMessage = new TopicMessage("test-msg", "test-source", "test-type", "spec-vers", "data-content-type",
            "test-topic", "test-pubsub")
        {
            Data = "data"u8.ToArray()
        };

        var result = await capturedHandler(testMessage, CancellationToken.None);

        Assert.Equal(TopicResponseAction.Success, result);
        await _sendPipeline.Received(1).Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>());
    }

    [Fact]
    public async Task ExecuteAsync_ShouldRunPublisherAndSubscriberConcurrently()
    {
        // Verifies that publishing and subscribing happen in parallel for optimal throughput
        var listStarted = new TaskCompletionSource<bool>();
        var subscribeStarted = new TaskCompletionSource<bool>();

        var source = Substitute.For<IFileAdapter>();
        source.ListAsync().Returns(_ =>
        {
            listStarted.SetResult(true);
            return new List<Adapters.Common.FileEntry>();
        });

        var subscriptionMock = Substitute.For<IAsyncDisposable>();
        _topicSubscriber.SubscribeAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<TopicMessageHandler>(),
                Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                subscribeStarted.SetResult(true);
                return subscriptionMock;
            });

        var lifecycle = Lifecycle(source);

        _ = Task.Run(async () => await lifecycle.ExecuteAsync(CancellationToken.None));

        await Task.WhenAll(listStarted.Task, subscribeStarted.Task);

        Assert.True(listStarted.Task.IsCompleted);
        Assert.True(subscribeStarted.Task.IsCompleted);
    }

    private TransactionalIntegrationLifecycle<Context> Lifecycle(IFileAdapter source, SweepCompletion? completion = null)
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
            MaxMessageProcessingTime = _options.MaxMessageProcessingTime
        };
        var receiveJob = new TransactionalIntegrationReceiveJob<Context>(provider, Identity, NewContext, _loggerFactory);
        return new TransactionalIntegrationLifecycle<Context>(receiveJob, _topicSubscriber, _sendPipeline, options, "test-integration", NewContext, _loggerFactory);
    }

    public sealed record OrderContext(Dictionary<string, string> Metadata, bool IsRetry) : Context(Metadata, IsRetry);
}
