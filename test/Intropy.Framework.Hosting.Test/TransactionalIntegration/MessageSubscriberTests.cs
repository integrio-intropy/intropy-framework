using System.Diagnostics;
using CloudNative.CloudEvents;
using Grpc.Core;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Intropy.Framework.Testing.Delivery;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

/// <summary>
/// The send side of one Transactional Integration run, through its real app callback: it serves
/// the callback while the run lasts, waits for publishing before the idle timeout can end the run,
/// tallies each message's latest outcome, and drains before it stops.
/// </summary>
public class MessageSubscriberTests
{
    private const string PubSub = "internal-test-integration";
    private const string Topic = "hop";

    private readonly ISendPipeline<Context> _pipeline = Substitute.For<ISendPipeline<Context>>();

    public MessageSubscriberTests() => PipelineReturns(_ => new StepResult<string>.Success(""));

    private static TransactionalIntegrationOptions Options(TimeSpan? idleTimeout = null,
        TimeSpan? gracePeriod = null) => new()
    {
        DaprPubSubName = PubSub,
        DaprTopicName = Topic,
        IdleTimeout = idleTimeout ?? TimeSpan.FromMilliseconds(500),
        PostIdleGracePeriod = gracePeriod ?? TimeSpan.FromSeconds(5),
        MaxMessageProcessingTime = TimeSpan.FromSeconds(30),
        CallbackPort = AppCallbackDelivery.AvailablePort()
    };

    private MessageSubscriber<Context> Subscriber(TransactionalIntegrationOptions options,
        TimeProvider? time = null)
    {
        var processor = new MessageProcessor<Context>(_pipeline, (metadata, isRetry) => new Context(metadata, isRetry),
            "test-integration", NullLogger<MessageProcessor<Context>>.Instance);
        return new MessageSubscriber<Context>(processor, options, "test-integration", NullLoggerFactory.Instance, time);
    }

    /// <summary>The send pipeline's result for each message, by its message id.</summary>
    private void PipelineReturns(Func<string, StepResult<string>> result) =>
        _pipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>(), Arg.Any<CancellationToken>())
            .Returns(call => (result(call.Arg<Context>().Metadata[ContextKeys.MessageId]), call.Arg<Context>()));

    private static CloudEvent Event(string id) => new()
    {
        Id = id, Source = new Uri("urn:test"), Type = "file.received", DataContentType = "application/json",
        Data = new { File = "orders.csv" }
    };

    /// <summary>A delivery to the run's callback, once it listens.</summary>
    private static async Task<AppCallbackDelivery> ConnectAsync(TransactionalIntegrationOptions options)
    {
        var delivery = new AppCallbackDelivery(options.CallbackPort!.Value, PubSub, Topic);
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

    [Fact]
    public async Task ExecuteAsync_WaitsForPublishingToComplete_BeforeTheIdleTimeoutCanEndTheRun()
    {
        var options = Options(idleTimeout: TimeSpan.FromMilliseconds(100));
        var publishing = new TaskCompletionSource();

        var run = Subscriber(options).ExecuteAsync(publishing.Task);
        await Task.Delay(500);
        Assert.False(run.IsCompleted);
        publishing.SetResult();

        await run.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task ExecuteAsync_OpensAFreshIdleWindow_WhenMonitoringStarts()
    {
        // A sweep outlasting the idle timeout must not look already-idle at the first poll: the
        // window is measured from when monitoring starts, not from when the run began.
        var time = new FakeTimeProvider();
        var options = Options(idleTimeout: TimeSpan.FromSeconds(5));
        var publishing = new TaskCompletionSource();
        var run = Subscriber(options, time).ExecuteAsync(publishing.Task);
        using var _ = await ConnectAsync(options);

        time.Advance(TimeSpan.FromSeconds(10));
        publishing.SetResult();
        await Task.Delay(100);
        time.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(100);

        Assert.False(run.IsCompleted);

        time.Advance(TimeSpan.FromSeconds(5));
        await run.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task ExecuteAsync_ProcessesPushedMessages_AndSummarizesTheRun()
    {
        PipelineReturns(id => id switch
        {
            "duplicate" => new StepResult<string>.Cancelled(),
            "failing" => new StepResult<string>.TechnicalFailure(new TechnicalFailure("send failed")),
            _ => new StepResult<string>.Success("")
        });
        var options = Options();
        var publishing = new TaskCompletionSource();
        var run = Subscriber(options).ExecuteAsync(publishing.Task);
        using var delivery = await ConnectAsync(options);

        var acks = new[]
        {
            await delivery.DeliverAsync(Event("processed")),
            await delivery.DeliverAsync(Event("duplicate")),
            await delivery.DeliverAsync(Event("failing"))
        };
        publishing.SetResult();
        var summary = await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal([DeliveryAck.Success, DeliveryAck.Success, DeliveryAck.Retry], acks);
        Assert.Equal(new RunSummary(Processed: 1, Failed: 1, Skipped: 1), summary);
    }

    [Fact]
    public async Task ExecuteAsync_CountsEachMessagesLatestOutcome()
    {
        var attempts = 0;
        PipelineReturns(_ => Interlocked.Increment(ref attempts) == 1
            ? new StepResult<string>.TechnicalFailure(new TechnicalFailure("send failed"))
            : new StepResult<string>.Success(""));
        var options = Options();
        var publishing = new TaskCompletionSource();
        var run = Subscriber(options).ExecuteAsync(publishing.Task);
        using var delivery = await ConnectAsync(options);

        await delivery.DeliverAsync(Event("msg-1"));
        await delivery.DeliverAsync(Event("msg-1"), redelivery: true);
        publishing.SetResult();
        var summary = await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(new RunSummary(Processed: 1, Failed: 0, Skipped: 0), summary);
    }

    [Fact]
    public async Task ExecuteAsync_LinksEachMessagesSpanToTheRun()
    {
        using var jobSource = new ActivitySource($"subscriber-link-test-{Guid.NewGuid()}");
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source == jobSource || source.Name == "Intropy.Framework.Hosting",
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        Activity? consumer = null;
        _pipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                consumer = Activity.Current;
                return ((StepResult<string>)new StepResult<string>.Success(""), call.Arg<Context>());
            });
        var options = Options();
        var publishing = new TaskCompletionSource();

        Task<RunSummary> run;
        ActivitySpanId jobSpanId;
        using (var job = jobSource.StartActivity("job")!)
        {
            jobSpanId = job.SpanId;
            run = Subscriber(options).ExecuteAsync(publishing.Task);
        }
        using var delivery = await ConnectAsync(options);
        await delivery.DeliverAsync(Event("msg-1"));
        publishing.SetResult();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.NotNull(consumer);
        Assert.Contains(consumer.Links, l => l.Context.SpanId == jobSpanId);
    }

    [Fact]
    public async Task ExecuteAsync_WhenTheHostStops_InterruptsAMessageOutlivingTheGracePeriodWithoutCountingIt()
    {
        // Host cancellation is not a failure (the runner exits 0), but the message still goes back
        // for redelivery.
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _pipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<Context>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                entered.TrySetResult();
                await Task.Delay(Timeout.Infinite, call.Arg<CancellationToken>());
                return ((StepResult<string>)new StepResult<string>.Success(""), call.Arg<Context>());
            });
        var options = Options(gracePeriod: TimeSpan.FromMilliseconds(200));
        using var host = new CancellationTokenSource();
        var run = Subscriber(options).ExecuteAsync(new TaskCompletionSource().Task, host.Token);
        using var delivery = await ConnectAsync(options);

        var ack = delivery.DeliverAsync(Event("msg-1"));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await host.CancelAsync();
        var summary = await run.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(DeliveryAck.Retry, await ack.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(RunSummary.Empty, summary);
    }

    [Fact]
    public async Task ExecuteAsync_StopsServingTheCallback_WhenTheRunEnds()
    {
        var options = Options();

        await Subscriber(options).ExecuteAsync(Task.CompletedTask).WaitAsync(TimeSpan.FromSeconds(10));

        using var delivery = new AppCallbackDelivery(options.CallbackPort!.Value, PubSub, Topic);
        var refused = await Assert.ThrowsAsync<RpcException>(() => delivery.GetSubscriptionsAsync());
        Assert.Equal(StatusCode.Unavailable, refused.StatusCode);
    }
}
