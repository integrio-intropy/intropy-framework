using System.Diagnostics;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.BulkLoader.Steps;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Blocks.Test.Loader;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Test.BulkLoader;

internal sealed class BatchCustomerSender(
    Func<IReadOnlyList<(LoaderCustomerOut Value, Context Context)>, IReadOnlyList<StepResult<LoaderCustomerOut>>>? respond = null)
    : BatchSendStep<LoaderCustomerOut, Context>
{
    public List<IReadOnlyList<(LoaderCustomerOut Value, Context Context)>> Calls { get; } = [];

    public override Task<IReadOnlyList<StepResult<LoaderCustomerOut>>> ExecuteBatchAsync(
        IReadOnlyList<(LoaderCustomerOut Value, Context Context)> entries, CancellationToken ct)
    {
        Calls.Add(entries);
        var response = respond?.Invoke(entries)
            ?? entries.Select(e => (StepResult<LoaderCustomerOut>)new StepResult<LoaderCustomerOut>.Success(e.Value))
                .ToList();
        return Task.FromResult(response);
    }
}

/// <summary>Deserializes as <see cref="CustomerDeserializer"/> does, taking a while about it, and
/// records how many messages it was deserializing at once.</summary>
internal sealed class ConcurrencyTrackingDeserializer : DeserializeStep<LoaderCustomerIn, Context>
{
    private readonly CustomerDeserializer _inner = new();
    private int _running;
    private int _maxRunning;

    public int MaxRunning => Volatile.Read(ref _maxRunning);

    public override async Task<(BusinessStepResult<LoaderCustomerIn> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct)
    {
        var running = Interlocked.Increment(ref _running);
        int max;
        while (running > (max = Volatile.Read(ref _maxRunning)) &&
               Interlocked.CompareExchange(ref _maxRunning, running, max) != max)
        {
        }

        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
            return await _inner.ExecuteAsync(input, context, ct);
        }
        finally
        {
            Interlocked.Decrement(ref _running);
        }
    }
}

public class BulkLoaderTests
{
    private readonly IIdempotencyServiceClient _idempotencyServiceClient;
    private readonly IBusinessIncidentServiceClient _businessIncidentServiceClient;
    private readonly IServiceProvider _serviceProvider;

    public BulkLoaderTests()
    {
        _idempotencyServiceClient = Substitute.For<IIdempotencyServiceClient>();
        _businessIncidentServiceClient = Substitute.For<IBusinessIncidentServiceClient>();

        _idempotencyServiceClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));
        _businessIncidentServiceClient.Trigger(Arg.Any<Uri>(), Arg.Any<string>(), Arg.Any<string>(),
                Arg.Any<BusinessIncidentData>(), Arg.Any<string?>())
            .ReturnsForAnyArgs(Task.CompletedTask);

        var services = new ServiceCollection();
        services.AddSingleton(_idempotencyServiceClient);
        services.AddSingleton(_businessIncidentServiceClient);
        services.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "BulkLoaderTest";
            conf.ServiceNamespace = "Org";
        });
        services.AddLogging();
        _serviceProvider = services.BuildServiceProvider();
    }

    private static Context NewContext(string id) => new(new Dictionary<string, string> { ["message_id"] = id });

    private static ReadOnlyMemory<byte> Payload(string customerId, string name) =>
        System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new { customerId, name });

    private Intropy.Framework.Blocks.BulkLoader.BulkLoaderBuilder<LoaderCustomerIn, LoaderCustomerOut, Context> Builder(
        BatchCustomerSender sender) =>
        Intropy.Framework.Blocks.BulkLoader.BulkLoaderBuilder<LoaderCustomerIn, LoaderCustomerOut, Context>
            .Create("BulkLoaderTest", _serviceProvider)
            .WithDeserializer(new CustomerDeserializer())
            .WithValidator(new CustomerValidator())
            .WithIdempotency((input, _) => input.CustomerId, (_, _) => DateTimeOffset.UtcNow)
            .WithTransformer(new CustomerTransformer())
            .WithSender(sender)
            .WithBusinessIncidents(
                ctx => ctx.Metadata.GetValueOrDefault("message_id", "unknown"),
                ctx => ctx.Metadata.GetValueOrDefault("message_id", "unknown"));

    [Fact]
    public async Task Execute_WithAllValidMessages_ProcessesTheWholeBatchSuccessfully()
    {
        var sender = new BatchCustomerSender();
        var pipeline = Builder(sender).Build();

        var batch = new[]
        {
            (Payload("1", "Alice"), NewContext("m1")),
            (Payload("2", "Bob"), NewContext("m2")),
            (Payload("3", "Carol"), NewContext("m3"))
        };

        var results = await pipeline.Execute(batch);

        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.IsType<StepResult<LoaderCustomerOut>.Success>(r.Result));
        Assert.Single(sender.Calls); // one combined call, not one per message
        Assert.Equal(3, sender.Calls[0].Count);
        await _idempotencyServiceClient.Received(3).CommitAsync(Arg.Any<MessageInfo>());
    }

    [Fact]
    public async Task Execute_WithOneInvalidMessage_ExcludesItFromTheBatchSend_OthersStillSucceed()
    {
        var sender = new BatchCustomerSender();
        var pipeline = Builder(sender).Build();

        var batch = new[]
        {
            (Payload("1", "Alice"), NewContext("m1")),
            (Payload("invalid", "Bob"), NewContext("m2")), // fails Validate
            (Payload("3", "Carol"), NewContext("m3"))
        };

        var results = await pipeline.Execute(batch);

        Assert.Equal(3, results.Count);
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(results[0].Result);
        Assert.IsType<StepResult<LoaderCustomerOut>.RoutedIncident>(results[1].Result);
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(results[2].Result);

        // Only the two valid messages ever reached the batch send.
        Assert.Single(sender.Calls);
        Assert.Equal(2, sender.Calls[0].Count);
        await _businessIncidentServiceClient.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Execute_WithDuplicateMessage_ExcludesItFromTheBatchSend_AndCompletesAsCancelled()
    {
        _idempotencyServiceClient.GetStatusAsync(Arg.Is<MessageInfo>(m => m.Id == "2"))
            .Returns(Task.FromResult(new StatusResponse(Action.Ignore, Reason.SameData)));

        var sender = new BatchCustomerSender();
        var pipeline = Builder(sender).Build();

        var batch = new[]
        {
            (Payload("1", "Alice"), NewContext("m1")),
            (Payload("2", "Bob"), NewContext("m2")), // already processed
            (Payload("3", "Carol"), NewContext("m3"))
        };

        var results = await pipeline.Execute(batch);

        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(results[0].Result);
        Assert.IsType<StepResult<LoaderCustomerOut>.Cancelled>(results[1].Result);
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(results[2].Result);
        Assert.Equal(2, sender.Calls[0].Count);
        await _idempotencyServiceClient.DidNotReceive().CommitAsync(Arg.Is<MessageInfo>(m => m.Id == "2"));
    }

    [Fact]
    public async Task Execute_WithPartialSendRejection_RoutesAnIncidentOnlyForTheRejectedEntry()
    {
        var sender = new BatchCustomerSender(entries => entries.Select(e =>
            e.Value.CustomerId == 2
                ? (StepResult<LoaderCustomerOut>)new StepResult<LoaderCustomerOut>.BusinessFailure(
                    new BusinessIncidentData { Description = "rejected by downstream" }, Retriable: false)
                : new StepResult<LoaderCustomerOut>.Success(e.Value)).ToList());
        var pipeline = Builder(sender).Build();

        var batch = new[]
        {
            (Payload("1", "Alice"), NewContext("m1")),
            (Payload("2", "Bob"), NewContext("m2")),
        };

        var results = await pipeline.Execute(batch);

        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(results[0].Result);
        Assert.IsType<StepResult<LoaderCustomerOut>.RoutedIncident>(results[1].Result);
        await _idempotencyServiceClient.Received(1).CommitAsync(Arg.Any<MessageInfo>()); // only the successful entry
        await _businessIncidentServiceClient.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Execute_WithWholeBatchSendFailure_RoutesAnIncidentForEveryEntry()
    {
        var sender = new BatchCustomerSender(_ => throw new TimeoutException("downstream unreachable"));
        var pipeline = Builder(sender).Build();

        var batch = new[]
        {
            (Payload("1", "Alice"), NewContext("m1")),
            (Payload("2", "Bob"), NewContext("m2")),
        };

        var results = await pipeline.Execute(batch);

        // Both entries get the same (retriable, default) failure, so neither is converted to RoutedIncident —
        // routed as an incident for visibility, and still left for redelivery.
        Assert.All(results, r => Assert.IsType<StepResult<LoaderCustomerOut>.BusinessFailure>(r.Result));
        await _idempotencyServiceClient.DidNotReceive().CommitAsync(Arg.Any<MessageInfo>());
        await _businessIncidentServiceClient.Received(2).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    private static (ReadOnlyMemory<byte>, Context)[] FourCustomers() =>
    [
        (Payload("1", "Alice"), NewContext("m1")), (Payload("2", "Bob"), NewContext("m2")),
        (Payload("3", "Carol"), NewContext("m3")), (Payload("4", "Dave"), NewContext("m4"))
    ];

    [Fact]
    public async Task Execute_ByDefault_ProcessesOneMessageAtATime()
    {
        var deserializer = new ConcurrencyTrackingDeserializer();
        var pipeline = Builder(new BatchCustomerSender()).WithDeserializer(deserializer).Build();

        await pipeline.Execute(FourCustomers());

        Assert.Equal(1, deserializer.MaxRunning);
    }

    [Fact]
    public async Task Execute_WithParallelism_ProcessesThatManyMessagesAtATime()
    {
        var deserializer = new ConcurrencyTrackingDeserializer();
        var pipeline = Builder(new BatchCustomerSender()).WithDeserializer(deserializer)
            .WithParallelism(preBatch: 4).Build();

        await pipeline.Execute(FourCustomers());

        Assert.InRange(deserializer.MaxRunning, 2, 4);
    }

    [Fact]
    public async Task Execute_WithEmptyBatch_ReturnsEmptyResults_AndNeverCallsTheSender()
    {
        var sender = new BatchCustomerSender();
        var pipeline = Builder(sender).Build();

        var results = await pipeline.Execute([]);

        Assert.Empty(results);
        Assert.Empty(sender.Calls);
    }

    // Tracing: each message gets one pipeline span, open across the batch send, in the trace of the
    // parent it is given; the send is traced once, in a trace of its own, linked both ways.

    private static ActivityContext RemoteParent() =>
        new(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded, isRemote: true);

    [Fact]
    public async Task Execute_WithAParentPerMessage_TracesEachMessageInItsOwnTrace_UnderOneSpan()
    {
        var parents = new[] { RemoteParent(), RemoteParent(), RemoteParent() };
        using var spans = new SpanRecorder([.. parents.Select(p => p.TraceId)]);
        var pipeline = Builder(new BatchCustomerSender()).Build();

        await pipeline.Execute(
            [(Payload("1", "Alice"), NewContext("m1")), (Payload("2", "Bob"), NewContext("m2")),
             (Payload("3", "Carol"), NewContext("m3"))], parents);

        foreach (var parent in parents)
        {
            var trace = spans.InTrace(parent.TraceId);
            var message = Assert.Single(trace, s => s.DisplayName == "Pipeline.BulkLoaderTest");
            Assert.Equal(parent.SpanId, message.ParentSpanId);
            // Every step, before the send and after it, sits directly under the message's one span.
            Assert.All(trace.Where(s => s != message), s => Assert.Equal(message.SpanId, s.ParentSpanId));
            Assert.Contains(trace, s => s.DisplayName == "Step.BulkSend");
            Assert.Contains(trace, s => s.DisplayName == "Finalizer.RouteBusinessIncidents");
        }
    }

    [Fact]
    public async Task Execute_TracesTheBatchSendInATraceOfItsOwn_LinkedBothWaysToEachMessage()
    {
        var parents = new[] { RemoteParent(), RemoteParent() };
        using var spans = new SpanRecorder([.. parents.Select(p => p.TraceId)]);
        var pipeline = Builder(new BatchCustomerSender()).Build();

        await pipeline.Execute([(Payload("1", "Alice"), NewContext("m1")), (Payload("2", "Bob"), NewContext("m2"))],
            parents);

        var messages = parents.Select(p => spans.InTrace(p.TraceId).Single(s => s.DisplayName == "Pipeline.BulkLoaderTest"))
            .ToList();
        var batch = spans.BatchLinkedTo(messages[0]);
        Assert.Equal(default, batch.ParentSpanId);
        Assert.DoesNotContain(batch.TraceId, parents.Select(p => p.TraceId));
        Assert.Equal(2, batch.GetTagItem("messaging.batch.message_count"));
        Assert.Equal(2, batch.GetTagItem("intropy.batch.succeeded_count"));
        Assert.Equal(messages.Select(m => m.SpanId.ToHexString()).Order(),
            batch.Links.Select(l => l.Context.SpanId.ToHexString()).Order());

        foreach (var message in messages)
        {
            // The message's part in the call spans the call itself, carries the message's own verdict
            // and leads back to the call.
            var send = spans.InTrace(message.TraceId).Single(s => s.DisplayName == "Step.BulkSend");
            Assert.Equal(message.SpanId, send.ParentSpanId);
            Assert.Equal(batch.StartTimeUtc, send.StartTimeUtc);
            Assert.Equal(batch.Duration, send.Duration);
            Assert.Equal("success", send.GetTagItem("step.result"));
            Assert.Equal(batch.SpanId, Assert.Single(send.Links).Context.SpanId);
        }
    }

    [Fact]
    public async Task Execute_GivesEachMessageItsOwnVerdictOnItsPartInTheSend()
    {
        var parents = new[] { RemoteParent(), RemoteParent() };
        using var spans = new SpanRecorder([.. parents.Select(p => p.TraceId)]);
        var sender = new BatchCustomerSender(entries => entries.Select(e => e.Value.CustomerId == 2
            ? new StepResult<LoaderCustomerOut>.BusinessFailure(new BusinessIncidentData { Description = "rejected" })
            : (StepResult<LoaderCustomerOut>)new StepResult<LoaderCustomerOut>.Success(e.Value)).ToList());
        var pipeline = Builder(sender).Build();

        await pipeline.Execute([(Payload("1", "Alice"), NewContext("m1")), (Payload("2", "Bob"), NewContext("m2"))],
            parents);

        Assert.Equal("success", spans.InTrace(parents[0].TraceId).Single(s => s.DisplayName == "Step.BulkSend")
            .GetTagItem("step.result"));
        var rejected = spans.InTrace(parents[1].TraceId).Single(s => s.DisplayName == "Step.BulkSend");
        Assert.Equal("business_failure", rejected.GetTagItem("step.result"));
        Assert.Equal("rejected", rejected.GetTagItem("step.business_failure_message"));
    }

    [Fact]
    public async Task Execute_LeavesAMessageThatFailedBeforeTheSendOutOfTheSendsTraces()
    {
        var parents = new[] { RemoteParent(), RemoteParent() };
        using var spans = new SpanRecorder([.. parents.Select(p => p.TraceId)]);
        var pipeline = Builder(new BatchCustomerSender()).Build();

        await pipeline.Execute(
            [(Payload("1", "Alice"), NewContext("m1")), (Payload("invalid", "Bob"), NewContext("m2"))], parents);

        var invalid = spans.InTrace(parents[1].TraceId);
        var message = Assert.Single(invalid, s => s.DisplayName == "Pipeline.BulkLoaderTest");
        Assert.DoesNotContain(invalid, s => s.DisplayName == "Step.BulkSend");
        Assert.Contains(invalid, s => s.DisplayName == "Finalizer.RouteBusinessIncidents");
        var sent = spans.InTrace(parents[0].TraceId).Single(s => s.DisplayName == "Pipeline.BulkLoaderTest");
        var batch = spans.BatchLinkedTo(sent);
        Assert.Equal(sent.SpanId, Assert.Single(batch.Links).Context.SpanId);
        Assert.DoesNotContain(batch.Links, l => l.Context.SpanId == message.SpanId);
    }

    [Fact]
    public async Task Execute_WithoutParents_TracesEachMessageUnderTheCurrentSpan()
    {
        using var caller = new Activity("caller").Start();
        using var spans = new SpanRecorder(caller.TraceId);
        var pipeline = Builder(new BatchCustomerSender()).Build();

        await pipeline.Execute([(Payload("1", "Alice"), NewContext("m1")), (Payload("2", "Bob"), NewContext("m2"))]);

        var trace = spans.InTrace(caller.TraceId);
        var messages = trace.Where(s => s.DisplayName == "Pipeline.BulkLoaderTest").ToList();
        Assert.Equal(2, messages.Count);
        Assert.All(messages, m => Assert.Equal(caller.SpanId, m.ParentSpanId));
        Assert.All(messages, m => Assert.Single(trace, s => s.DisplayName == "Step.BulkSend" && s.ParentSpanId == m.SpanId));
        Assert.Same(caller, Activity.Current);
    }

    [Fact]
    public async Task Execute_WhenTheSendFailsTheBatch_EndsEveryMessagesSpanAsAnError()
    {
        var parents = new[] { RemoteParent(), RemoteParent() };
        using var spans = new SpanRecorder([.. parents.Select(p => p.TraceId)]);
        var pipeline = Builder(new BatchCustomerSender(_ => [])).Build();

        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.Execute(
            [(Payload("1", "Alice"), NewContext("m1")), (Payload("2", "Bob"), NewContext("m2"))], parents));

        foreach (var parent in parents)
        {
            var trace = spans.InTrace(parent.TraceId);
            Assert.Equal(ActivityStatusCode.Error,
                trace.Single(s => s.DisplayName == "Pipeline.BulkLoaderTest").Status);
            Assert.Equal(ActivityStatusCode.Error, trace.Single(s => s.DisplayName == "Step.BulkSend").Status);
        }

        var sent = spans.InTrace(parents[0].TraceId).Single(s => s.DisplayName == "Pipeline.BulkLoaderTest");
        Assert.Equal(ActivityStatusCode.Error, spans.BatchLinkedTo(sent).Status);
    }

    /// <summary>Records the spans of the traces a test starts, and of the batch sends linked to them,
    /// leaving every other test's spans to that test's own sampling.</summary>
    private sealed class SpanRecorder : IDisposable
    {
        private readonly List<Activity> _stopped = [];
        private readonly ActivityListener _listener;

        public SpanRecorder(params ActivityTraceId[] traces)
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name.StartsWith("Intropy.Framework.", StringComparison.Ordinal),
                Sample = (ref ActivityCreationOptions<ActivityContext> options) =>
                    traces.Contains(options.TraceId) ||
                    (options.Links?.Any(l => traces.Contains(l.Context.TraceId)) ?? false)
                        ? ActivitySamplingResult.AllDataAndRecorded
                        : ActivitySamplingResult.None,
                ActivityStopped = activity =>
                {
                    lock (_stopped) _stopped.Add(activity);
                }
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public List<Activity> InTrace(ActivityTraceId traceId)
        {
            lock (_stopped) return [.. _stopped.Where(s => s.TraceId == traceId)];
        }

        /// <summary>The batch send span that links to <paramref name="message"/>.</summary>
        public Activity BatchLinkedTo(Activity message)
        {
            lock (_stopped)
                return _stopped.Single(s => s.DisplayName == "Step.BulkSend" &&
                                            s.Links.Any(l => l.Context.SpanId == message.SpanId));
        }

        public void Dispose() => _listener.Dispose();
    }
}
