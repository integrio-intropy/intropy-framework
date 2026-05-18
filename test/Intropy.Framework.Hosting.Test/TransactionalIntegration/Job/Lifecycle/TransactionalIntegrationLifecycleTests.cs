using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.TransactionalIntegration.Job;
using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Job.Lifecycle;

public class TransactionalIntegrationLifecycleTests
{
    private readonly ISourceLister _sourceLister = Substitute.For<ISourceLister>();
    private readonly IReceivePipeline<Context> _receivePipeline = Substitute.For<IReceivePipeline<Context>>();
    private readonly ITopicSubscriber _topicSubscriber = Substitute.For<ITopicSubscriber>();
    private readonly ISendPipeline<Context> _sendPipeline = Substitute.For<ISendPipeline<Context>>();

    private readonly TransactionalIntegrationOptions _options = new()
    {
        DaprPubSubName = "test-pubsub",
        DaprTopicName = "test-topic",
        IdleTimeoutSeconds = 1,
        PostIdleGracePeriodSeconds = 1,
        MaxMessageProcessingTimeSeconds = 30
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
    public async Task Start_ShouldProcessAllSourceItems_ThroughReceivePipeline()
    {
        // Verifies that all source items are processed through the receive pipeline
        var items = new List<SourceItemInfo>
        {
            new("file1.txt"),
            new("file2.txt")
        };

        _sourceLister.ListItemsAsync(Arg.Any<CancellationToken>())
            .Returns(items);

        _receivePipeline.Execute(Arg.Any<SourceItemInfo>(), Arg.Any<Context>())
            .Returns(callInfo =>
            {
                var item = callInfo.Arg<SourceItemInfo>();
                return (new StepResult<SourceItem>.Success(new SourceItem(item.Id, [])), callInfo.Arg<Context>());
            });

        var lifecycle = new TransactionalIntegrationLifecycle(
            _sourceLister, _receivePipeline, _topicSubscriber,
            _sendPipeline, _options, _loggerFactory);

        await lifecycle.Start();

        await _receivePipeline.Received(2).Execute(Arg.Any<SourceItemInfo>(), Arg.Any<Context>());
    }

    [Fact]
    public async Task Start_ShouldSubscribeToMessages()
    {
        // Verifies that the lifecycle subscribes to the configured topic for message processing
        _sourceLister.ListItemsAsync(Arg.Any<CancellationToken>())
            .Returns([]);

        var lifecycle = new TransactionalIntegrationLifecycle(
            _sourceLister, _receivePipeline, _topicSubscriber,
            _sendPipeline, _options, _loggerFactory);

        await lifecycle.Start();

        await _topicSubscriber.Received(1).SubscribeAsync(
            "test-pubsub",
            "test-topic",
            Arg.Any<TimeSpan>(),
            Arg.Any<TopicMessageHandler>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Start_ShouldCompleteSuccessfully_WhenNoSourceItemsExist()
    {
        // Verifies graceful handling when there are no source items to process
        _sourceLister.ListItemsAsync(Arg.Any<CancellationToken>())
            .Returns([]);

        var lifecycle = new TransactionalIntegrationLifecycle(
            _sourceLister, _receivePipeline, _topicSubscriber,
            _sendPipeline, _options, _loggerFactory);

        await lifecycle.Start();

        await _receivePipeline.DidNotReceiveWithAnyArgs().Execute(default!, default!);
    }

    [Fact]
    public async Task Start_ShouldProcessReceivedMessages()
    {
        // Verifies that messages received from the queue are passed to the message handler
        _sourceLister.ListItemsAsync(Arg.Any<CancellationToken>())
            .Returns([]);

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

        var lifecycle = new TransactionalIntegrationLifecycle(
            _sourceLister, _receivePipeline, _topicSubscriber,
            _sendPipeline, _options, _loggerFactory);

        _ = Task.Run(async () => await lifecycle.Start());

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
    public async Task Start_ShouldRunPublisherAndSubscriberConcurrently()
    {
        // Verifies that publishing and subscribing happen in parallel for optimal throughput
        var listStarted = new TaskCompletionSource<bool>();
        var subscribeStarted = new TaskCompletionSource<bool>();

        _sourceLister.ListItemsAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            listStarted.SetResult(true);
            return [];
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

        var lifecycle = new TransactionalIntegrationLifecycle(
            _sourceLister, _receivePipeline, _topicSubscriber,
            _sendPipeline, _options, _loggerFactory);

        _ = Task.Run(async () => await lifecycle.Start());

        await Task.WhenAll(listStarted.Task, subscribeStarted.Task);

        Assert.True(listStarted.Task.IsCompleted);
        Assert.True(subscribeStarted.Task.IsCompleted);
    }
}
