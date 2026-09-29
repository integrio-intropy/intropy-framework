using System.Diagnostics;
using Intropy.Contracts.BusinessIncidentService;
using Dapr.Messaging.PublishSubscribe;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

public class MessageSubscriberTests
{
    private readonly ITopicSubscriber _topicSubscriber = Substitute.For<ITopicSubscriber>();
    private readonly ISendPipeline<Context> _messageHandler = Substitute.For<ISendPipeline<Context>>();

    private readonly TransactionalIntegrationOptions _options = new()
    {
        DaprPubSubName = "test-pubsub",
        DaprTopicName = "test-topic",
        IdleTimeout = TimeSpan.FromSeconds(1),
        PostIdleGracePeriod = TimeSpan.FromSeconds(1),
        MaxMessageProcessingTime = TimeSpan.FromSeconds(30)
    };

    private readonly ILogger<MessageSubscriber<Context>> _logger = Substitute.For<ILogger<MessageSubscriber<Context>>>();

    /// <summary>Builds a subscriber around the shared send-pipeline substitute, composing the
    /// processor the way the DI registration does.</summary>
    private MessageSubscriber<Context> CreateSubscriber(ITopicSubscriber topicSubscriber,
        TransactionalIntegrationOptions? options = null, TimeProvider? timeProvider = null)
    {
        var resolved = options ?? _options;
        var processor = new MessageProcessor<Context>(_messageHandler,
            (metadata, isRetry) => new Context(metadata, isRetry), "test-integration", resolved.DaprTopicName,
            Substitute.For<ILogger<MessageProcessor<Context>>>());
        return new MessageSubscriber<Context>(topicSubscriber, processor, resolved, "test-integration",
            _logger, timeProvider);
    }

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

        var subscriber = CreateSubscriber(_topicSubscriber);
        var executeTask = Task.Run(async () => await subscriber.ExecuteAsync(publishingComplete.Task));

        await Task.Delay(200);
        Assert.False(executeTask.IsCompleted);

        publishingComplete.SetResult(true);
        await executeTask;

        Assert.True(executeTask.IsCompleted);
    }

    [Fact]
    public async Task ExecuteAsync_OpensAFreshIdleWindow_WhenMonitoringStarts()
    {
        // A sweep outlasting the idle timeout must not look already-idle at the first poll: the
        // window is measured from when monitoring starts, not from when the subscriber was built.
        var time = new FakeTimeProvider();
        var disposed = new TaskCompletionSource();
        var subscription = new SignalSubscription(disposed);
        TopicMessageHandler? handler = null;
        var topicSubscriber = Substitute.For<ITopicSubscriber>();
        topicSubscriber.SubscribeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(),
                Arg.Do((Action<TopicMessageHandler>)(h => handler = h)), Arg.Any<CancellationToken>())
            .Returns(subscription);

        var options = new TransactionalIntegrationOptions
        {
            DaprPubSubName = "test-pubsub",
            DaprTopicName = "test-topic",
            IdleTimeout = TimeSpan.FromSeconds(5)
        };
        var subscriber = CreateSubscriber(topicSubscriber, options, time);
        var publishing = new TaskCompletionSource();

        var run = subscriber.ExecuteAsync(publishing.Task);
        for (var i = 0; i < 20 && handler is null; i++)
            await Task.Delay(50);
        Assert.NotNull(handler);

        // The sweep runs longer than the idle timeout while the publisher is still going.
        time.Advance(TimeSpan.FromSeconds(10));
        publishing.SetResult();
        // Let monitoring start and open the fresh window before the first poll fires.
        await Task.Delay(100);

        time.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(100);

        // Assert — quiet time measured from monitoring start is one second, not eleven: the run
        // is still draining, not already shutting down.
        Assert.False(disposed.Task.IsCompleted);

        // Once the full idle window really elapses, the run shuts down and disposes cleanly.
        time.Advance(TimeSpan.FromSeconds(5));
        await disposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await run.WaitAsync(TimeSpan.FromSeconds(5));
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

        var subscriber = CreateSubscriber(_topicSubscriber);

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

        var subscriber = CreateSubscriber(_topicSubscriber);

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

    [Fact]
    public async Task HandleMessage_CreatesTheIntegrationsOwnContextFromThePropagatedMetadata()
    {
        // Verifies that the send side rebuilds the integration's context type from the message:
        // the metadata propagated from the receive side, the message id, and the retry flag
        var sendPipeline = Substitute.For<ISendPipeline<OrderContext>>();
        OrderContext? received = null;
        sendPipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Do<OrderContext>(c => received = c))
            .Returns(callInfo => ((StepResult<string>)new StepResult<string>.Success(""), callInfo.Arg<OrderContext>()));
        TopicMessageHandler? handler = null;
        var topicSubscriber = Substitute.For<ITopicSubscriber>();
        topicSubscriber.SubscribeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(),
                Arg.Do((Action<TopicMessageHandler>)(h => handler = h)), Arg.Any<CancellationToken>())
            .Returns(Substitute.For<IAsyncDisposable>());
        var processor = new MessageProcessor<OrderContext>(sendPipeline,
            (metadata, isRetry) => new OrderContext(metadata, isRetry), "test-integration", _options.DaprTopicName,
            Substitute.For<ILogger<MessageProcessor<OrderContext>>>());
        var subscriber = new MessageSubscriber<OrderContext>(topicSubscriber, processor, _options, "test-integration",
            Substitute.For<ILogger<MessageSubscriber<OrderContext>>>());
        _ = Task.Run(async () => await subscriber.ExecuteAsync(Task.CompletedTask));
        for (var i = 0; i < 20 && handler is null; i++)
            await Task.Delay(50);
        var message = CreateTopicMessage("msg-1", "data"u8.ToArray(), new Dictionary<string, Value>
        {
            ["metadata"] = Value.ForString("{\"file_name\":\"orders.csv\"}"),
            ["retrycount"] = Value.ForString("1")
        });

        await handler!(message, CancellationToken.None);

        Assert.NotNull(received);
        Assert.Equal("orders.csv", received.Metadata[SourceContextKeys.FileName]);
        Assert.Equal("msg-1", received.Metadata[ContextKeys.MessageId]);
        Assert.True(received.IsRetry);
    }

    public static TheoryData<StepResult<string>, string> FailedResults => new()
    {
        { new StepResult<string>.TechnicalFailure(new TechnicalFailure("send failed")), "send failed" },
        { new StepResult<string>.BusinessFailure(new BusinessIncidentData { Description = "order rejected", Context = [] }), "order rejected" }
    };

    [Theory]
    [MemberData(nameof(FailedResults))]
    public async Task HandleMessage_MarksTheConsumerSpanAsAnError_WhenThePipelineFails(StepResult<string> failure,
        string description)
    {
        // A retried message was not processed, whatever kind of failure caused it.
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(Task.FromResult((failure, new Context(new Dictionary<string, string>()))));

        var span = await CaptureConsumerSpanAsync();

        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal(description, span.StatusDescription);
    }

    [Fact]
    public async Task HandleMessage_RecordsTheExceptionOnTheConsumerSpan_WhenThePipelineThrows()
    {
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(Task.FromException<(StepResult<string>, Context)>(
                new InvalidOperationException("Processing failed")));

        var span = await CaptureConsumerSpanAsync();

        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Contains(span.Events, e => e.Name == "exception");
    }

    [Fact]
    public async Task HandleMessage_LeavesTheConsumerSpanUnset_WhenThePipelineSucceeds()
    {
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(Task.FromResult(((StepResult<string>)new StepResult<string>.Success(""),
                new Context(new Dictionary<string, string>()))));

        var span = await CaptureConsumerSpanAsync();

        Assert.Equal(ActivityStatusCode.Unset, span.Status);
    }

    /// <summary>Delivers a message carrying a fresh trace parent and returns the consumer span in
    /// that trace only, so spans from tests running in parallel are ignored.</summary>
    private async Task<Activity> CaptureConsumerSpanAsync()
    {
        var traceId = ActivityTraceId.CreateRandom();
        var traceParent = $"00-{traceId}-{ActivitySpanId.CreateRandom()}-01";
        var stopped = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Intropy.Framework.Hosting",
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (stopped) stopped.Add(activity); }
        };
        ActivitySource.AddActivityListener(listener);

        await InvokeMessageHandler(_topicSubscriber, CreateTopicMessage("test-id", "data"u8.ToArray(),
            new Dictionary<string, Value> { ["traceparent"] = Value.ForString(traceParent) }));

        lock (stopped)
            return Assert.Single(stopped, a => a.DisplayName == "process test-topic" && a.TraceId == traceId);
    }

    public static TheoryData<string?> MissingOrInvalidTraceParents => new() { null, "", "not-a-traceparent" };

    [Theory]
    [MemberData(nameof(MissingOrInvalidTraceParents))]
    public async Task HandleMessage_StartsANewTrace_WhenTheMessageCarriesNoValidTraceContext(string? traceParent)
    {
        // Even when a span (such as the job's) is current on the delivering thread, a message
        // without trace context is its own unit of work, not part of that span's trace.
        using var jobSource = new ActivitySource($"subscriber-tracing-test-{Guid.NewGuid()}");
        using var listener = ListenTo(jobSource);
        var extensions = traceParent is null
            ? new Dictionary<string, Value>()
            : new Dictionary<string, Value> { ["traceparent"] = Value.ForString(traceParent) };

        using var job = jobSource.StartActivity("job")!;
        var consumer = await CaptureSpanDuringPipelineAsync(CreateTopicMessage("test-id", "data"u8.ToArray(), extensions));

        Assert.NotNull(consumer);
        Assert.Equal("process test-topic", consumer.DisplayName);
        Assert.Equal(ActivityKind.Consumer, consumer.Kind);
        Assert.Equal(default, consumer.ParentSpanId);
        Assert.NotEqual(job.TraceId, consumer.TraceId);
        Assert.Same(job, Activity.Current);
    }

    [Fact]
    public async Task HandleMessage_ContinuesThePropagatedTrace_WhenTheMessageCarriesOne()
    {
        using var listener = ListenTo(null);
        var traceId = ActivityTraceId.CreateRandom();
        var parentSpanId = ActivitySpanId.CreateRandom();
        var extensions = new Dictionary<string, Value>
        {
            ["traceparent"] = Value.ForString($"00-{traceId}-{parentSpanId}-01")
        };

        var consumer = await CaptureSpanDuringPipelineAsync(CreateTopicMessage("test-id", "data"u8.ToArray(), extensions));

        Assert.NotNull(consumer);
        Assert.Equal(traceId, consumer.TraceId);
        Assert.Equal(parentSpanId, consumer.ParentSpanId);
        Assert.True(consumer.HasRemoteParent);
    }

    [Fact]
    public async Task HandleMessage_DescribesTheConsumerSpanWithTheMessagingConventions()
    {
        using var listener = ListenTo(null);
        var extensions = new Dictionary<string, Value> { ["retrycount"] = Value.ForString("2") };

        var consumer = await CaptureSpanDuringPipelineAsync(CreateTopicMessage("msg-7", "data"u8.ToArray(), extensions));

        Assert.NotNull(consumer);
        Assert.Equal("dapr", consumer.GetTagItem("messaging.system"));
        Assert.Equal("process", consumer.GetTagItem("messaging.operation.type"));
        Assert.Equal("test-topic", consumer.GetTagItem("messaging.destination.name"));
        Assert.Equal("msg-7", consumer.GetTagItem("messaging.message.id"));
        // The component is the resource's service.name, on every span already.
        Assert.Null(consumer.GetTagItem("intropy.component.name"));
        Assert.Equal(2L, consumer.GetTagItem("intropy.message.retry_count"));
    }

    [Fact]
    public async Task HandleMessage_LinksTheConsumerSpanToTheRunThatConsumedIt()
    {
        // The subscription is opened inside the job's span: every message it consumes links to it,
        // whether or not the message continues a trace of its own
        using var jobSource = new ActivitySource($"subscriber-link-test-{Guid.NewGuid()}");
        using var listener = ListenTo(jobSource);
        TopicMessageHandler? handler = null;
        var topicSubscriber = Substitute.For<ITopicSubscriber>();
        topicSubscriber.SubscribeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(),
                Arg.Do((Action<TopicMessageHandler>)(h => handler = h)), Arg.Any<CancellationToken>())
            .Returns(Substitute.For<IAsyncDisposable>());
        Activity? consumer = null;
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(_ =>
            {
                consumer = Activity.Current;
                return ((StepResult<string>)new StepResult<string>.Success(""), new Context(new Dictionary<string, string>()));
            });
        var subscriber = CreateSubscriber(topicSubscriber);
        var publishing = new TaskCompletionSource();

        Task run;
        ActivitySpanId jobSpanId;
        using (var job = jobSource.StartActivity("job")!)
        {
            jobSpanId = job.SpanId;
            run = subscriber.ExecuteAsync(publishing.Task);
        }
        for (var i = 0; i < 20 && handler is null; i++)
            await Task.Delay(50);
        await handler!(CreateTopicMessage("msg-1", "data"u8.ToArray()), CancellationToken.None);
        publishing.SetResult();
        await run;

        Assert.NotNull(consumer);
        Assert.Contains(consumer.Links, l => l.Context.SpanId == jobSpanId);
    }

    [Fact]
    public async Task HandleMessage_TagsTheErrorTypeOnTheConsumerSpan_WhenThePipelineFails()
    {
        using var listener = ListenTo(null);
        Activity? consumer = null;
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(_ =>
            {
                consumer = Activity.Current;
                return ((StepResult<string>)new StepResult<string>.TechnicalFailure(new TechnicalFailure("send failed")),
                    new Context(new Dictionary<string, string>()));
            });

        await InvokeMessageHandler(_topicSubscriber, CreateTopicMessage("test-id", "data"u8.ToArray()));

        Assert.NotNull(consumer);
        Assert.Equal("technical_failure", consumer.GetTagItem("error.type"));
    }

    [Fact]
    public async Task HandleMessage_RetriesAndCountsAsFailed_WhenThePipelineAbortsWithoutTheHostStopping()
    {
        // For example the processing timeout: the message was not processed, so it must not be
        // acknowledged (which would drop it), and the run must not look clean
        using var listener = ListenTo(null);

        var (response, summary, consumer) = await RunWithOneMessageAsync(_ => new StepResult<string>.Aborted());

        Assert.Equal(TopicResponseAction.Retry, response);
        Assert.Equal(1, summary.Failed);
        Assert.Equal(0, summary.Processed);
        Assert.NotNull(consumer);
        Assert.Equal(ActivityStatusCode.Error, consumer.Status);
        Assert.Equal("aborted", consumer.GetTagItem("error.type"));
    }

    [Fact]
    public async Task HandleMessage_RetriesWithoutCountingIt_WhenThePipelineAbortsBecauseTheHostIsStopping()
    {
        // Host cancellation is not a failure (the runner exits 0), but the message still goes back
        // for redelivery
        using var listener = ListenTo(null);

        var (response, summary, consumer) = await RunWithOneMessageAsync(host =>
        {
            host.Cancel();
            return new StepResult<string>.Aborted();
        });

        Assert.Equal(TopicResponseAction.Retry, response);
        Assert.Equal(RunSummary.Empty, summary);
        Assert.NotNull(consumer);
        Assert.Equal(ActivityStatusCode.Unset, consumer.Status);
    }

    [Fact]
    public async Task HandleMessage_RetriesWithoutCountingIt_WhenThePipelineThrowsACancellationBecauseTheHostIsStopping()
    {
        using var listener = ListenTo(null);

        var (response, summary, consumer) = await RunWithOneMessageAsync(host =>
        {
            host.Cancel();
            throw new OperationCanceledException(host.Token);
        });

        Assert.Equal(TopicResponseAction.Retry, response);
        Assert.Equal(RunSummary.Empty, summary);
        Assert.NotNull(consumer);
        Assert.Equal(ActivityStatusCode.Unset, consumer.Status);
    }

    public static TheoryData<string, string, string?> ResultsAndOutcomes => new()
    {
        { "success", "processed", null },
        { "duplicate", "skipped", null },
        { "technical", "failed", "technical_failure" },
        { "aborted", "failed", "aborted" },
        { "host-stopping", "interrupted", null }
    };

    [Theory]
    [MemberData(nameof(ResultsAndOutcomes))]
    public async Task HandleMessage_RecordsTheConsumedMessageAndItsProcessingDuration(string result, string outcome,
        string? errorType)
    {
        using var metrics = new MetricCapture();
        var options = new TransactionalIntegrationOptions
        {
            DaprPubSubName = _options.DaprPubSubName,
            DaprTopicName = $"metrics-topic-{Guid.NewGuid()}",
            IdleTimeout = _options.IdleTimeout,
            PostIdleGracePeriod = _options.PostIdleGracePeriod,
            MaxMessageProcessingTime = _options.MaxMessageProcessingTime
        };

        await RunWithOneMessageAsync(host => result switch
        {
            "success" => new StepResult<string>.Success(""),
            "duplicate" => new StepResult<string>.Cancelled(),
            "technical" => new StepResult<string>.TechnicalFailure(new TechnicalFailure("send failed")),
            "aborted" => new StepResult<string>.Aborted(),
            _ => CancelAndAbort(host)
        }, options);

        var consumed = Assert.Single(metrics.Of("messaging.client.consumed.messages", "messaging.destination.name",
            options.DaprTopicName));
        Assert.Equal(1, consumed.Value);
        Assert.Equal("dapr", consumed.Tags["messaging.system"]);
        Assert.Equal("process", consumed.Tags["messaging.operation.name"]);
        Assert.Equal(outcome, consumed.Tags["intropy.message.outcome"]);
        Assert.Equal("test-integration", consumed.Tags["intropy.component.name"]);
        Assert.Equal(errorType, consumed.Tags.GetValueOrDefault("error.type"));
        var duration = Assert.Single(metrics.Of("messaging.process.duration", "messaging.destination.name",
            options.DaprTopicName));
        Assert.Equal(outcome, duration.Tags["intropy.message.outcome"]);
    }

    private static StepResult<string> CancelAndAbort(CancellationTokenSource host)
    {
        host.Cancel();
        return new StepResult<string>.Aborted();
    }

    /// <summary>Runs a subscriber whose publisher has finished, delivers one message whose send
    /// pipeline returns <paramref name="result"/> (given the host's cancellation source), and
    /// returns the response to Dapr, the run's summary, and the consumer span.</summary>
    private async Task<(TopicResponseAction Response, RunSummary Summary, Activity? Consumer)> RunWithOneMessageAsync(
        Func<CancellationTokenSource, StepResult<string>> result, TransactionalIntegrationOptions? options = null)
    {
        using var host = new CancellationTokenSource();
        TopicMessageHandler? handler = null;
        var topicSubscriber = Substitute.For<ITopicSubscriber>();
        topicSubscriber.SubscribeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(),
                Arg.Do((Action<TopicMessageHandler>)(h => handler = h)), Arg.Any<CancellationToken>())
            .Returns(Substitute.For<IAsyncDisposable>());
        Activity? consumer = null;
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(_ =>
            {
                consumer = Activity.Current;
                return (result(host), new Context(new Dictionary<string, string>()));
            });
        var subscriber = CreateSubscriber(topicSubscriber, options);
        // The publisher never finishes on its own: the run ends on the idle timeout, or when the
        // pipeline cancels the host.
        var publishing = new TaskCompletionSource();

        var run = subscriber.ExecuteAsync(publishing.Task, host.Token);
        for (var i = 0; i < 20 && handler is null; i++)
            await Task.Delay(50);
        var response = await handler!(CreateTopicMessage("msg-1", "data"u8.ToArray()), CancellationToken.None);
        publishing.TrySetResult();

        return (response, await run.WaitAsync(TimeSpan.FromSeconds(10)), consumer);
    }

    /// <summary>Delivers <paramref name="message"/> and returns the span current while the send
    /// pipeline ran: the consumer span.</summary>
    private async Task<Activity?> CaptureSpanDuringPipelineAsync(TopicMessage message)
    {
        Activity? current = null;
        _messageHandler.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>())
            .Returns(_ =>
            {
                current = Activity.Current;
                return ((StepResult<string>)new StepResult<string>.Success(""), new Context(new Dictionary<string, string>()));
            });

        await InvokeMessageHandler(_topicSubscriber, message);
        return current;
    }

    private static ActivityListener ListenTo(ActivitySource? testSource)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source == testSource || source.Name == "Intropy.Framework.Hosting",
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private sealed class SignalSubscription(TaskCompletionSource disposed) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            disposed.SetResult();
            return ValueTask.CompletedTask;
        }
    }

    public sealed record OrderContext(Dictionary<string, string> Metadata, bool IsRetry) : Context(Metadata, IsRetry);

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

        var subscriber = CreateSubscriber(topicSubscriber);

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
