using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Test.Loader;

/// <summary>
/// The batch loader over a batch of thin change events: filtered, coalesced by entity, looked up in
/// chunks, then sent per entity — with one result per entry.
/// </summary>
public class BatchLoaderTests
{
    private readonly IIdempotencyServiceClient _idempotency = Substitute.For<IIdempotencyServiceClient>();
    private readonly IBusinessIncidentServiceClient _incidents = Substitute.For<IBusinessIncidentServiceClient>();
    private readonly FakeLookup _lookup = new();
    private readonly BatchSender _sender = new();
    private readonly IServiceProvider _provider;

    public BatchLoaderTests()
    {
        _idempotency.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));
        var services = new ServiceCollection();
        services.AddSingleton(_idempotency);
        services.AddSingleton(_incidents);
        services.AddSingleton(Substitute.For<ILoggerFactory>());
        services.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "BatchTest";
            conf.ServiceNamespace = "Org";
        });
        _provider = services.BuildServiceProvider();
    }

    [Fact]
    public async Task Execute_SendsEveryEntityAndReturnsOneResultPerEntryInOrder()
    {
        var loader = Builder().Build();

        var results = await loader.ExecuteAsync([Entry("e1", "P1"), Entry("e2", "P2")]);

        Assert.Equal(["e1", "e2"], results.Select(r => r.EntryId));
        Assert.All(results, r => Assert.IsType<StepResult<Product>.Success>(r.Result));
        Assert.Equal(["P1", "P2"], _sender.Sent.Select(p => p.Id).Order());
    }

    [Fact]
    public async Task Execute_CoalescesEntriesForOneEntityIntoOneRun()
    {
        var loader = Builder().Build();

        var results = await loader.ExecuteAsync([
            Entry("e1", "P1", minutes: 1), Entry("e2", "P1", minutes: 3), Entry("e3", "P1", minutes: 2)
        ]);

        // One lookup key and one send for the entity; every entry shares the result.
        Assert.Equal(["P1"], Assert.Single(_lookup.Calls));
        Assert.Single(_sender.Sent);
        Assert.All(results, r => Assert.IsType<StepResult<Product>.Success>(r.Result));
        // The latest event is the one that runs: its time is the idempotency date.
        await _idempotency.Received(1).CommitAsync(Arg.Is<MessageInfo>(m =>
            m.Timestamp == BaseTime.AddMinutes(3)));
    }

    [Fact]
    public async Task Execute_ConsumesFilteredEntriesWithoutLookingThemUp()
    {
        var loader = Builder().Where(e => e.ProductId != "SKIP").Build();

        var results = await loader.ExecuteAsync([Entry("e1", "SKIP"), Entry("e2", "P2")]);

        Assert.True(results[0].Filtered);
        Assert.IsType<StepResult<Product>.Cancelled>(results[0].Result);
        Assert.Equal(["P2"], Assert.Single(_lookup.Calls));
    }

    [Fact]
    public async Task Execute_LooksUpInChunksOfTheConfiguredSize()
    {
        var loader = Builder(chunkSize: 2).Build();

        await loader.ExecuteAsync([.. Enumerable.Range(1, 5).Select(i => Entry($"e{i}", $"P{i}"))]);

        Assert.Equal([2, 2, 1], _lookup.Calls.Select(c => c.Count));
    }

    [Fact]
    public async Task Execute_WhenALookupChunkFails_FailsOnlyThatChunksEntries()
    {
        _lookup.FailWhen = keys => keys.Contains("P1");
        var loader = Builder(chunkSize: 1).Build();

        var results = await loader.ExecuteAsync([Entry("e1", "P1"), Entry("e2", "P2")]);

        Assert.IsType<StepResult<Product>.TechnicalFailure>(results[0].Result);
        Assert.IsType<StepResult<Product>.Success>(results[1].Result);
        Assert.Equal(["P2"], _sender.Sent.Select(p => p.Id));
    }

    [Theory]
    [InlineData(MissingItemPolicy.Retry, typeof(StepResult<Product>.TechnicalFailure), false)]
    [InlineData(MissingItemPolicy.Incident, typeof(StepResult<Product>.Success), true)]
    [InlineData(MissingItemPolicy.Skip, typeof(StepResult<Product>.Cancelled), false)]
    public async Task Execute_WhenTheLookupMissesAnEntity_AppliesTheMissingPolicy(MissingItemPolicy policy,
        Type expected, bool incident)
    {
        var loader = Builder(missing: policy).Build();

        var results = await loader.ExecuteAsync([Entry("e1", "GONE")]);

        Assert.IsType(expected, results[0].Result);
        Assert.Empty(_sender.Sent);
        await _incidents.Received(incident ? 1 : 0).Trigger(Arg.Any<Uri>(), Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Execute_WhenAnEntryCannotBeDeserialized_RoutesAnIncidentAndProcessesTheOthers()
    {
        var loader = Builder().Build();
        var broken = new BatchEntry<Context>("e1", new CloudEvent
        {
            Id = "e1", Type = "product.changed", Source = new Uri("urn:test"), Time = BaseTime, Data = ""
        }, new Context(new Dictionary<string, string>()));

        var results = await loader.ExecuteAsync([broken, Entry("e2", "P2")]);

        // The incident finalizer consumes the business failure.
        Assert.IsType<StepResult<Product>.Success>(results[0].Result);
        Assert.IsType<StepResult<Product>.Success>(results[1].Result);
        await _incidents.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        Assert.Equal(["P2"], _sender.Sent.Select(p => p.Id));
    }

    [Fact]
    public async Task Execute_KeysIdempotencyOnTheEntityScopedByRoute()
    {
        var loader = Builder(routeEventType: "product.changed").Build();

        await loader.ExecuteAsync([Entry("e1", "P1")]);

        await _idempotency.Received(1).CommitAsync(Arg.Is<MessageInfo>(m => m.Id == "product.changed:P1"));
    }

    [Fact]
    public async Task Execute_WhenTheLookedUpStateIsUnchanged_SkipsTheEntity()
    {
        _idempotency.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Ignore, Reason.SameData)));
        var loader = Builder().Build();

        var results = await loader.ExecuteAsync([Entry("e1", "P1")]);

        Assert.IsType<StepResult<Product>.Cancelled>(results[0].Result);
        Assert.Empty(_sender.Sent);
    }

    [Fact]
    public async Task Execute_RunsAtMostMaxConcurrencyEntitiesAtATime()
    {
        _sender.Delay = TimeSpan.FromMilliseconds(30);
        var loader = Builder().WithMaxConcurrency(3).Build();

        await loader.ExecuteAsync([.. Enumerable.Range(1, 9).Select(i => Entry($"e{i}", $"P{i}"))]);

        Assert.Equal(9, _sender.Sent.Count);
        Assert.InRange(_sender.MaxInFlight, 2, 3);
    }

    [Fact]
    public void Build_WithoutALookup_RequiresTheEnrichedTypeToBeTheInput()
    {
        var builder = BatchLoaderBuilder<ProductChanged, Product, Product, Context>.Create("BatchTest", _provider)
            .WithDeserializer(new ChangeDeserializer()).KeyedBy(e => e.ProductId)
            .WithValidator(new ProductValidator()).WithIdempotency().WithTransformer(new ProductTransformer())
            .WithSender(_sender).WithBusinessIncidents(_ => "id", _ => "subject");

        var error = Assert.Throws<InvalidOperationException>(builder.Build);
        Assert.Contains("WithLookup", error.Message, StringComparison.Ordinal);
    }

    private static readonly DateTimeOffset BaseTime = new(2026, 9, 29, 10, 0, 0, TimeSpan.Zero);

    private BatchLoaderBuilder<ProductChanged, Product, Product, Context> Builder(int chunkSize = 100,
        MissingItemPolicy missing = MissingItemPolicy.Retry, string? routeEventType = null)
    {
        var builder = routeEventType is null
            ? BatchLoaderBuilder<ProductChanged, Product, Product, Context>.Create("BatchTest", _provider)
            : BatchLoaderBuilder<ProductChanged, Product, Product, Context>.Create("BatchTest", _provider, routeEventType);
        return builder
            .WithDeserializer(new ChangeDeserializer())
            .KeyedBy(e => e.ProductId)
            .WithLookup(_lookup, chunkSize, missing)
            .WithValidator(new ProductValidator())
            .WithIdempotency()
            .WithTransformer(new ProductTransformer())
            .WithSender(_sender)
            .WithBusinessIncidents(_ => "message-id", _ => "subject");
    }

    private static BatchEntry<Context> Entry(string entryId, string productId, int minutes = 0) =>
        new(entryId, new CloudEvent
        {
            Id = entryId,
            Type = "product.changed",
            Source = new Uri("urn:test"),
            Subject = productId,
            Time = BaseTime.AddMinutes(minutes),
            Data = JsonSerializer.Serialize(new ProductChanged(productId))
        }, new Context(new Dictionary<string, string>()));

    public sealed record ProductChanged(string ProductId);

    public sealed record Product(string Id, string Name);

    private sealed class ChangeDeserializer : DeserializeStep<ProductChanged, Context>
    {
        protected override Task<(BusinessStepResult<ProductChanged> Result, Context Context)> DeserializeAsync(
            CloudEvent cloudEvent, Context context)
        {
            var json = cloudEvent.Data?.ToString();
            BusinessStepResult<ProductChanged> result = string.IsNullOrEmpty(json)
                ? new BusinessStepResult<ProductChanged>.Failure(new BusinessIncidentData
                {
                    Description = "No data", Context = new Dictionary<string, string>()
                })
                : new BusinessStepResult<ProductChanged>.Success(JsonSerializer.Deserialize<ProductChanged>(json)!);
            return Task.FromResult((result, context));
        }
    }

    private sealed class FakeLookup : BatchLookupStep<Product>
    {
        private readonly List<IReadOnlyList<string>> _calls = [];

        public IReadOnlyList<IReadOnlyList<string>> Calls
        {
            get { lock (_calls) return [.. _calls]; }
        }

        public Func<IReadOnlyList<string>, bool>? FailWhen { get; set; }

        public override Task<IReadOnlyDictionary<string, Product>> LookupAsync(IReadOnlyList<string> keys,
            CancellationToken cancellationToken)
        {
            lock (_calls) _calls.Add([.. keys]);
            if (FailWhen?.Invoke(keys) == true)
                throw new InvalidOperationException("The source is down");
            IReadOnlyDictionary<string, Product> found = keys.Where(k => k != "GONE")
                .ToDictionary(k => k, k => new Product(k, $"Product {k}"));
            return Task.FromResult(found);
        }
    }

    private sealed class ProductValidator : ValidateStep<Product, Context>
    {
        public override Task<(BusinessStepResult<Product> Result, Context Context)> ExecuteAsync(Product input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<Product>, Context)>((new BusinessStepResult<Product>.Success(input), context));
    }

    private sealed class ProductTransformer : TransformStep<Product, Product, Context>
    {
        public override Task<(TechnicalStepResult<Product> Result, Context Context)> ExecuteAsync(Product input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<Product>, Context)>((new TechnicalStepResult<Product>.Success(input), context));
    }

    private sealed class BatchSender : SendStep<Product, Context>
    {
        private readonly List<Product> _sent = [];
        private int _inFlight;

        public IReadOnlyList<Product> Sent
        {
            get { lock (_sent) return [.. _sent]; }
        }

        public TimeSpan Delay { get; set; }

        public int MaxInFlight { get; private set; }

        public override async Task<(TechnicalStepResult<Product> Result, Context Context)> ExecuteAsync(Product input,
            Context context, CancellationToken ct)
        {
            var inFlight = Interlocked.Increment(ref _inFlight);
            lock (_sent) MaxInFlight = Math.Max(MaxInFlight, inFlight);
            try
            {
                if (Delay > TimeSpan.Zero)
                    await Task.Delay(Delay, ct);
                lock (_sent) _sent.Add(input);
                return (new TechnicalStepResult<Product>.Success(input), context);
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }
    }
}
