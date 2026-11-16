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

/// <summary>Records how many calls were running at once, each taking a while.</summary>
internal sealed class ConcurrencyGauge
{
    private int _running;
    private int _maxRunning;

    public int MaxRunning => Volatile.Read(ref _maxRunning);

    public async Task<T> TrackAsync<T>(Func<Task<T>> call, CancellationToken ct)
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
            return await call();
        }
        finally
        {
            Interlocked.Decrement(ref _running);
        }
    }
}

/// <summary>Deserializes as <see cref="CustomerDeserializer"/> does, taking a while about it, and
/// records how many messages it was deserializing at once.</summary>
internal sealed class ConcurrencyTrackingDeserializer : DeserializeStep<LoaderCustomerIn, Context>
{
    private readonly CustomerDeserializer _inner = new();
    private readonly ConcurrencyGauge _gauge = new();

    public int MaxRunning => _gauge.MaxRunning;

    public override Task<(BusinessStepResult<LoaderCustomerIn> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
        _gauge.TrackAsync(() => _inner.ExecuteAsync(input, context, ct), ct);
}

/// <summary>Passes each message through, taking a while about it, and records how many it was
/// handling at once.</summary>
internal sealed class ConcurrencyTrackingExtractor : ExtractStep<LoaderCustomerIn, Context>
{
    private readonly ConcurrencyGauge _gauge = new();

    public int MaxRunning => _gauge.MaxRunning;

    public override Task<(BusinessStepResult<LoaderCustomerIn> Result, Context Context)> ExecuteAsync(
        LoaderCustomerIn input, Context context, CancellationToken ct) =>
        _gauge.TrackAsync(() => Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
            (new BusinessStepResult<LoaderCustomerIn>.Success(input), context)), ct);
}

/// <summary>Logs each message it sees, to show the order the extractors run in.</summary>
internal sealed class LoggingExtractor(string name, List<string> log) : ExtractStep<LoaderCustomerIn, Context>
{
    public override Task<(BusinessStepResult<LoaderCustomerIn> Result, Context Context)> ExecuteAsync(
        LoaderCustomerIn input, Context context, CancellationToken ct)
    {
        lock (log) log.Add($"{name}:{input.CustomerId}");
        return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
            (new BusinessStepResult<LoaderCustomerIn>.Success(input), context));
    }
}

/// <summary>Fills in each customer's name from a directory in one call, as a batch lookup would. A
/// customer the directory does not know fails on its own.</summary>
internal sealed class BatchNameLookup(IReadOnlyDictionary<string, string> directory, string name = "lookup",
    List<string>? log = null) : BatchExtractStep<LoaderCustomerIn, Context>
{
    public List<IReadOnlyList<string>> Calls { get; } = [];
    public Exception? Throws { get; init; }
    public bool Miscounts { get; init; }

    public override Task<IReadOnlyList<StepResult<LoaderCustomerIn>>> ExecuteBatchAsync(
        IReadOnlyList<(LoaderCustomerIn Value, Context Context)> entries, CancellationToken ct)
    {
        Calls.Add([.. entries.Select(e => e.Value.CustomerId)]);
        log?.Add(name);
        if (Throws is not null)
            throw Throws;
        if (Miscounts)
            return Task.FromResult<IReadOnlyList<StepResult<LoaderCustomerIn>>>([]);

        return Task.FromResult<IReadOnlyList<StepResult<LoaderCustomerIn>>>([
            .. entries.Select(e => directory.TryGetValue(e.Value.CustomerId, out var found)
                ? (StepResult<LoaderCustomerIn>)new StepResult<LoaderCustomerIn>.Success(
                    new LoaderCustomerIn { CustomerId = e.Value.CustomerId, Name = found })
                : new StepResult<LoaderCustomerIn>.BusinessFailure(
                    new BusinessIncidentData { Description = "unknown customer" }, Retriable: false))
        ]);
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
            .WithBatchSender(sender)
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
        // Each message's record carries its own id, kept on its context across the batch send.
        foreach (var id in new[] { "1", "2", "3" })
            await _idempotencyServiceClient.Received(1).CommitAsync(Arg.Is<MessageInfo>(m => m.Id == id));
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
            .WithParallelism(4).Build();

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

    // Batch extractors: one call for every message still succeeding, in the order the extractors are
    // added, before validation.

    private static readonly Dictionary<string, string> s_directory = new()
    {
        ["1"] = "Alice", ["2"] = "Bob", ["3"] = "Carol", ["4"] = "Dave"
    };

    /// <summary>Messages that carry only a customer id; the name is the batch lookup's to fill in.</summary>
    private static (ReadOnlyMemory<byte>, Context)[] References(params string[] ids) =>
        [.. ids.Select(id => (Payload(id, ""), NewContext($"m{id}")))];

    [Fact]
    public async Task Execute_WithABatchExtractor_LooksUpEveryMessageInOneCall_BeforeValidation()
    {
        var lookup = new BatchNameLookup(s_directory);
        var sender = new BatchCustomerSender();
        var pipeline = Builder(sender).WithBatchExtractor(lookup).Build();

        var results = await pipeline.Execute(References("1", "2", "3"));

        // The validator rejects an empty name, so every message passing it saw the lookup's result.
        Assert.All(results, r => Assert.IsType<StepResult<LoaderCustomerOut>.Success>(r.Result));
        Assert.Equal(["1", "2", "3"], Assert.Single(lookup.Calls));
        Assert.Equal(["Alice", "Bob", "Carol"], Assert.Single(sender.Calls).Select(e => e.Value.Name));
    }

    [Fact]
    public async Task Execute_WhenTheBatchExtractorRejectsOneEntry_OnlyThatMessageFails_AndIsNotSent()
    {
        var sender = new BatchCustomerSender();
        var pipeline = Builder(sender).WithBatchExtractor(new BatchNameLookup(s_directory)).Build();

        var results = await pipeline.Execute(References("1", "9", "3"));

        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(results[0].Result);
        Assert.IsType<StepResult<LoaderCustomerOut>.RoutedIncident>(results[1].Result);
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(results[2].Result);
        Assert.Equal([1, 3], Assert.Single(sender.Calls).Select(e => e.Value.CustomerId));
        await _businessIncidentServiceClient.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Execute_WhenTheBatchExtractorThrows_EveryMessageIsLeftForRetry_AndNothingIsSent()
    {
        var sender = new BatchCustomerSender();
        var lookup = new BatchNameLookup(s_directory) { Throws = new HttpRequestException("source unreachable") };
        var pipeline = Builder(sender).WithBatchExtractor(lookup).Build();

        var results = await pipeline.Execute(References("1", "2"));

        Assert.All(results, r =>
            Assert.True(Assert.IsType<StepResult<LoaderCustomerOut>.BusinessFailure>(r.Result).Retriable));
        Assert.Empty(sender.Calls);
        await _idempotencyServiceClient.DidNotReceive().CommitAsync(Arg.Any<MessageInfo>());
    }

    [Fact]
    public async Task Execute_RunsExtractorsInTheOrderTheyAreAdded_EachBatchExtractorOnce()
    {
        var log = new List<string>();
        var pipeline = Builder(new BatchCustomerSender())
            .WithExtractor(new LoggingExtractor("a", log))
            .WithBatchExtractor(new BatchNameLookup(s_directory, "lookup", log))
            .WithExtractor(new LoggingExtractor("b", log))
            .WithBatchExtractor(new BatchNameLookup(s_directory, "again", log))
            .Build();

        await pipeline.Execute(References("1", "2"));

        Assert.Equal(["a:1", "a:2", "lookup", "b:1", "b:2", "again"], log);
    }

    [Fact]
    public async Task Execute_TheIdempotencyCheck_SeesWhatTheBatchExtractorFilledIn()
    {
        var seen = new List<string>();
        var pipeline = Builder(new BatchCustomerSender())
            .WithBatchExtractor(new BatchNameLookup(s_directory))
            .WithIdempotency((input, _) =>
            {
                lock (seen) seen.Add(input.Name);
                return input.CustomerId;
            }, (_, _) => DateTimeOffset.UtcNow)
            .Build();

        await pipeline.Execute(References("1", "2"));

        Assert.Equal(["Alice", "Bob"], seen.Distinct().Order());
    }

    [Fact]
    public async Task Execute_WithAPerMessageSender_SendsEachMessageOnItsOwn()
    {
        var lookup = new BatchNameLookup(s_directory);
        var sender = new CustomerSender();
        var pipeline = Builder(new BatchCustomerSender()).WithBatchExtractor(lookup).WithSender(sender).Build();

        var results = await pipeline.Execute(References("1", "2", "3"));

        Assert.All(results, r => Assert.IsType<StepResult<LoaderCustomerOut>.Success>(r.Result));
        Assert.Single(lookup.Calls);
        Assert.Equal(3, sender.SentCount);
        await _idempotencyServiceClient.Received(3).CommitAsync(Arg.Any<MessageInfo>());
    }

    [Fact]
    public async Task Execute_WithOnlyPerMessageSteps_LoadsEachMessageOnItsOwn()
    {
        var batchSender = new BatchCustomerSender();
        var sender = new CustomerSender();
        var pipeline = Builder(batchSender).WithSender(sender).Build();

        var results = await pipeline.Execute(FourCustomers());

        Assert.All(results, r => Assert.IsType<StepResult<LoaderCustomerOut>.Success>(r.Result));
        Assert.Equal(4, sender.SentCount);
        Assert.Empty(batchSender.Calls); // the sender configured last is the one used
    }

    [Fact]
    public async Task Execute_WithParallelism_AppliesToEveryStretchOfPerMessageSteps()
    {
        var extractor = new ConcurrencyTrackingExtractor();
        var pipeline = Builder(new BatchCustomerSender())
            .WithBatchExtractor(new BatchNameLookup(s_directory))
            .WithExtractor(extractor)
            .WithParallelism(4)
            .Build();

        await pipeline.Execute(References("1", "2", "3", "4"));

        Assert.InRange(extractor.MaxRunning, 2, 4);
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

    [Fact]
    public async Task Execute_TracesTheBatchExtractLikeTheSend_InATraceOfItsOwn_LinkedBothWays()
    {
        var parents = new[] { RemoteParent(), RemoteParent() };
        using var spans = new SpanRecorder([.. parents.Select(p => p.TraceId)]);
        var pipeline = Builder(new BatchCustomerSender()).WithBatchExtractor(new BatchNameLookup(s_directory)).Build();

        await pipeline.Execute(References("1", "2"), parents);

        var messages = parents.Select(p => spans.InTrace(p.TraceId).Single(s => s.DisplayName == "Pipeline.BulkLoaderTest"))
            .ToList();
        var lookup = spans.BatchLinkedTo(messages[0], "Step.BulkExtract");
        Assert.Equal(default, lookup.ParentSpanId);
        Assert.Equal(messages.Select(m => m.SpanId.ToHexString()).Order(),
            lookup.Links.Select(l => l.Context.SpanId.ToHexString()).Order());
        Assert.NotEqual(spans.BatchLinkedTo(messages[0]).TraceId, lookup.TraceId);

        foreach (var message in messages)
        {
            var trace = spans.InTrace(message.TraceId);
            var part = trace.Single(s => s.DisplayName == "Step.BulkExtract");
            Assert.Equal(lookup.StartTimeUtc, part.StartTimeUtc);
            Assert.Equal(lookup.SpanId, Assert.Single(part.Links).Context.SpanId);
            // Every step, on either side of both batch steps, sits directly under the message's one span.
            Assert.All(trace.Where(s => s != message), s => Assert.Equal(message.SpanId, s.ParentSpanId));
        }
    }

    [Fact]
    public async Task Execute_LeavesAMessageTheBatchExtractorRejectedOutOfTheSend()
    {
        var parents = new[] { RemoteParent(), RemoteParent() };
        using var spans = new SpanRecorder([.. parents.Select(p => p.TraceId)]);
        var pipeline = Builder(new BatchCustomerSender()).WithBatchExtractor(new BatchNameLookup(s_directory)).Build();

        await pipeline.Execute(References("1", "9"), parents);

        var rejected = spans.InTrace(parents[1].TraceId);
        Assert.Equal("business_failure",
            rejected.Single(s => s.DisplayName == "Step.BulkExtract").GetTagItem("step.result"));
        Assert.DoesNotContain(rejected, s => s.DisplayName == "Step.BulkSend");
    }

    [Fact]
    public async Task Execute_WhenTheBatchExtractorFailsTheBatch_EndsEveryMessagesSpanAsAnError()
    {
        var parents = new[] { RemoteParent(), RemoteParent() };
        using var spans = new SpanRecorder([.. parents.Select(p => p.TraceId)]);
        var lookup = new BatchNameLookup(s_directory) { Miscounts = true };
        var pipeline = Builder(new BatchCustomerSender()).WithBatchExtractor(lookup).Build();

        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.Execute(References("1", "2"), parents));

        foreach (var parent in parents)
        {
            var trace = spans.InTrace(parent.TraceId);
            Assert.Equal(ActivityStatusCode.Error,
                trace.Single(s => s.DisplayName == "Pipeline.BulkLoaderTest").Status);
            Assert.Equal(ActivityStatusCode.Error, trace.Single(s => s.DisplayName == "Step.BulkExtract").Status);
            Assert.DoesNotContain(trace, s => s.DisplayName == "Step.BulkSend");
        }
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

        /// <summary>The batch step's span, the send's by default, that links to
        /// <paramref name="message"/>.</summary>
        public Activity BatchLinkedTo(Activity message, string step = "Step.BulkSend")
        {
            lock (_stopped)
                return _stopped.Single(s => s.DisplayName == step &&
                                            s.Links.Any(l => l.Context.SpanId == message.SpanId));
        }

        public void Dispose() => _listener.Dispose();
    }
}
