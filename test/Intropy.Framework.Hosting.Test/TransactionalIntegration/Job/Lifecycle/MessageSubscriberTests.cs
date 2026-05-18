using Dapr.Messaging.PublishSubscribe;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.TransactionalIntegration.Job;
using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Job.Lifecycle;

public class MessageSubscriberTests
{
    private readonly ITopicSubscriber _topicSubscriber = Substitute.For<ITopicSubscriber>();
    private readonly ISendPipeline<Context> _messageHandler = Substitute.For<ISendPipeline<Context>>();

    private readonly TransactionalIntegrationOptions _options = new()
    {
        DaprPubSubName = "test-pubsub",
        DaprTopicName = "test-topic",
        IdleTimeoutSeconds = 1,
        PostIdleGracePeriodSeconds = 1,
        MaxMessageProcessingTimeSeconds = 30
    };

    private readonly ILogger<MessageSubscriber> _logger = Substitute.For<ILogger<MessageSubscriber>>();

    [Fact]
    public async Task ExecuteAsync_ShouldWaitForPublishingComplete_BeforeStartingIdleMonitor()
    {
        // Verifies that idle timeout monitoring only starts after all files have been published
        var publishingComplete = new TaskCompletionSource<bool>();
        var subscriptionMock = Substitute.For<IAsyncDisposable>();

        _topicSubscriber.SubscribeAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<TopicMessageHandler>(),
                Arg.Any<CancellationToken>())
            .Returns(subscriptionMock);

        var subscriber = new MessageSubscriber(_topicSubscriber, _messageHandler, _options, _logger);
        var executeTask = Task.Run(async () => await subscriber.ExecuteAsync(publishingComplete.Task));

        await Task.Delay(200);
        Assert.False(executeTask.IsCompleted);

        publishingComplete.SetResult(true);
        await executeTask;

        Assert.True(executeTask.IsCompleted);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldSubscribeToCorrectTopicAndPubSub()
    {
        // Verifies that the subscriber connects to the configured Dapr PubSub component and topic
        var publishingComplete = Task.CompletedTask;
        var subscriptionMock = Substitute.For<IAsyncDisposable>();

        _topicSubscriber.SubscribeAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<TopicMessageHandler>(),
                Arg.Any<CancellationToken>())
            .Returns(subscriptionMock);

        var subscriber = new MessageSubscriber(_topicSubscriber, _messageHandler, _options, _logger);

        await subscriber.ExecuteAsync(publishingComplete);

        await _topicSubscriber.Received(1).SubscribeAsync(
            "test-pubsub",
            "test-topic",
            Arg.Any<TimeSpan>(),
            Arg.Any<TopicMessageHandler>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_ShouldConfigureMaxProcessingTimeout()
    {
        // Verifies that message processing timeout is configured from options
        var publishingComplete = Task.CompletedTask;
        var subscriptionMock = Substitute.For<IAsyncDisposable>();
        TimeSpan? capturedMaxProcessingDuration = null;

        _topicSubscriber.SubscribeAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Do<TimeSpan>(opts => capturedMaxProcessingDuration = opts),
                Arg.Any<TopicMessageHandler>(),
                Arg.Any<CancellationToken>())
            .Returns(subscriptionMock);

        var subscriber = new MessageSubscriber(_topicSubscriber, _messageHandler, _options, _logger);

        await subscriber.ExecuteAsync(publishingComplete);

        Assert.NotNull(capturedMaxProcessingDuration);
        Assert.Equal(TimeSpan.FromSeconds(30), capturedMaxProcessingDuration);
    }

    [Fact]
    public async Task HandleMessage_ShouldReturnSuccess_WhenHandlerReturnsTrue()
    {
        // Verifies that successful message processing results in Success response to Dapr
        var pipelineResult = ((StepResult<string>)new StepResult<string>.Success(""),
            new Context(new Dictionary<string, string>()));
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(Task.FromResult(pipelineResult));

        var result = await InvokeMessageHandler(_topicSubscriber, CreateTopicMessage("test-id", "data"u8.ToArray()));

        Assert.Equal(TopicResponseAction.Success, result);
    }

    [Fact]
    public async Task HandleMessage_ShouldReturnRetry_WhenHandlerReturnsFalse()
    {
        // Verifies that failed message processing triggers a retry in Dapr
        var pipelineResult = (
            (StepResult<string>)new StepResult<string>.TechnicalFailure(
                new TechnicalFailure("")),
            new Context(new Dictionary<string, string>())
        );
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(Task.FromResult(pipelineResult));

        var result = await InvokeMessageHandler(_topicSubscriber, CreateTopicMessage("test-id", "data"u8.ToArray()));

        Assert.Equal(TopicResponseAction.Retry, result);
    }

    [Fact]
    public async Task HandleMessage_ShouldReturnRetry_WhenHandlerThrowsException()
    {
        // Verifies that exceptions during message processing trigger a retry instead of losing the message
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(Task.FromException<(StepResult<string>, Context)>(
                new InvalidOperationException("Processing failed")));

        var result = await InvokeMessageHandler(_topicSubscriber, CreateTopicMessage("test-id", "data"u8.ToArray()));

        Assert.Equal(TopicResponseAction.Retry, result);
    }

    [Fact]
    public async Task HandleMessage_ShouldPassMessageToHandler()
    {
        // Verifies that the received message is correctly passed to the message handler
        var pipelineResult = ((StepResult<string>)new StepResult<string>.Success(""),
            new Context(new Dictionary<string, string>()));
        ReadOnlyMemory<byte>? receivedMessage = null;
        _messageHandler.Execute(Arg.Do<ReadOnlyMemory<byte>>(m => receivedMessage = m), Arg.Any<Context>())
            .Returns(Task.FromResult(pipelineResult));
        const string messageId = "msg-123";

        var message = CreateTopicMessage(messageId, "data"u8.ToArray());

        await InvokeMessageHandler(_topicSubscriber, message);

        Assert.NotNull(receivedMessage);
    }

    [Fact]
    public async Task HandleMessage_ShouldCreateContextWithMessageId()
    {
        // Verifies that context is created with the message ID for tracking and correlation
        var pipelineResult = ((StepResult<string>)new StepResult<string>.Success(""),
            new Context(new Dictionary<string, string>()));
        Context? receivedContext = null;
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Do<Context>(c => receivedContext = c))
            .Returns(Task.FromResult(pipelineResult));
        const string messageId = "msg-456";

        var message = CreateTopicMessage(messageId, "data"u8.ToArray());

        await InvokeMessageHandler(_topicSubscriber, message);

        Assert.NotNull(receivedContext);
        Assert.Equal(messageId, receivedContext.Metadata[ContextKeys.MessageId]);
    }

    [Fact]
    public async Task HandleMessage_ShouldSetIsRetry_WhenRetryCountPresent()
    {
        // Verifies that retry messages are correctly flagged in the context
        var pipelineResult = ((StepResult<string>)new StepResult<string>.Success(""),
            new Context(new Dictionary<string, string>()));
        Context? receivedContext = null;
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Do<Context>(c => receivedContext = c))
            .Returns(Task.FromResult(pipelineResult));
        const string messageId = "msg-retry";
        var extensions = new Dictionary<string, Value> { { "retrycount", Value.ForString("1") } };

        var message = CreateTopicMessage(messageId, "data"u8.ToArray(), extensions);

        await InvokeMessageHandler(_topicSubscriber, message);

        Assert.NotNull(receivedContext);
        Assert.True(receivedContext.IsRetry);
    }

    private async Task<TopicResponseAction> InvokeMessageHandler(
        ITopicSubscriber topicSubscriber,
        TopicMessage message)
    {
        TopicMessageHandler? handler = null;
        var subscriptionMock = Substitute.For<IAsyncDisposable>();

        topicSubscriber.SubscribeAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Do((Action<TopicMessageHandler>)(h => handler = h)),
                Arg.Any<CancellationToken>())
            .Returns(subscriptionMock);

        var subscriber = new MessageSubscriber(topicSubscriber, _messageHandler, _options, _logger);

        var publishingComplete = Task.CompletedTask;
        _ = Task.Run(async () => await subscriber.ExecuteAsync(publishingComplete));

        // Wait for handler to be captured with retry
        for (var i = 0; i < 20 && handler is null; i++)
        {
            await Task.Delay(50);
        }

        Assert.NotNull(handler);
        return await handler(message, CancellationToken.None);
    }

    private static TopicMessage CreateTopicMessage(string id, byte[] data, Dictionary<string, Value>? extensions = null)
    {
        return new TopicMessage(id, Source: "test-source", Type: "test-type", SpecVersion: "",
            DataContentType: "",
            Topic: "", PubSubName: "") { Data = data, Extensions = extensions ?? new Dictionary<string, Value>(), };
    }
}
