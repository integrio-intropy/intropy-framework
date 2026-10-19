using System.Text.Json;
using Dapr.Client;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// The claim check as the hosting layer wires it: a registered claim store plus the component's mode
/// decide whether the pipeline the framework composes claim-checks, and a component never mentions
/// the mechanism in its own pipeline configuration.
/// </summary>
public class ExtractorClaimCheckWiringTests
{
    private sealed record Order(string OrderId, string Notes);

    private static readonly Order Big = new("ORD-1", new string('x', 4_000));

    /// <summary>A pipeline configured exactly as the scaffold template does: the publisher is a DI
    /// registration the tests can swap, handed to WithSender — so the builder never sees its source
    /// or type, and the claim check has to get them off the sender itself.</summary>
    private static ExtractorBuilder<Order, Order, Context> Pipeline(
        ExtractorBuilder<Order, Order, Context> builder, IServiceProvider provider) =>
        builder
            .WithDeserializer(new OrderDeserializer())
            .WithValidator(new PassingValidator())
            .WithIdempotency((o, _) => o.OrderId, (_, _) => DateTimeOffset.UtcNow)
            .WithTransformer(new IdentityTransformer())
            .WithSerializer(new CloudEventSerializeStep<Order, Context>(o => o.OrderId, _ => DateTimeOffset.UtcNow))
            .WithSender(provider.GetRequiredService<SendStep<Context>>())
            .WithBusinessIncidents(_ => "orders", _ => "orders");

    /// <summary>A pipeline that configures its own claim check against its own store, the way a
    /// component registering a prebuilt extractor would.</summary>
    private static ExtractorBuilder<Order, Order, Context> PipelineWithOwnClaimCheck(
        InMemoryFileAdapter ownStore, ExtractorBuilder<Order, Order, Context> builder,
        IServiceProvider provider) =>
        Pipeline(builder, provider).WithClaimCheck(ownStore, ClaimCheckPolicy.Always);

    private static ServiceProvider Services(Action<ExtractorOptions>? configure, bool withClaimStore,
        List<string>? log = null,
        Func<ExtractorBuilder<Order, Order, Context>, IServiceProvider, ExtractorBuilder<Order, Order, Context>>? pipeline = null)
    {
        var idempotency = Substitute.For<IIdempotencyServiceClient>();
        idempotency.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));

        var daprClient = Substitute.For<DaprClient>();
        daprClient.PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var services = new ServiceCollection();
        services.AddSingleton(idempotency);
        services.AddSingleton(Substitute.For<IBusinessIncidentServiceClient>());
        services.AddSingleton(daprClient);
        services.AddSingleton<SendStep<Context>>(sp => new DaprTopicPublisher<Context>(
            sp.GetRequiredService<DaprClient>(), "pubsub", "orders", new Uri("urn:example:orders"),
            "com.example.order"));
        if (log is null)
            services.AddLogging();
        else
            services.AddSingleton<ILoggerFactory>(new CapturingLoggerFactory(log));
        services.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "orders";
            conf.ServiceNamespace = "example";
        });
        if (withClaimStore)
            services.AddKeyedSingleton<IFileAdapter>(ClaimStoreServiceCollectionExtensions.ClaimStoreKey,
                new InMemoryFileAdapter());
        services.AddExtractor<Order, Order, Context>(pipeline ?? Pipeline,
            (metadata, isRetry) => new Context(metadata, isRetry), configureJob: null, configureOptions: configure);
        return services.BuildServiceProvider();
    }

    /// <summary>Runs one oversized payload through the composed pipeline and hands back the store,
    /// whose contents are the only observable that says whether the decorator was there.</summary>
    private static async Task<InMemoryFileAdapter> RunAsync(ServiceProvider provider)
    {
        var store = (InMemoryFileAdapter?)provider
            .GetKeyedService<IFileAdapter>(ClaimStoreServiceCollectionExtensions.ClaimStoreKey);
        using var scope = provider.CreateScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<Extractor<Order, Order, Context>>();
        await pipeline.Execute(JsonSerializer.SerializeToUtf8Bytes(Big), new Context([], false),
            ct: CancellationToken.None);
        return store ?? new InMemoryFileAdapter();
    }

    [Fact]
    public async Task AutoMode_WithAStore_ClaimChecksAnOversizedPayload()
    {
        var provider = Services(o => o.ClaimCheckThresholdBytes = 1_000, withClaimStore: true);

        var store = await RunAsync(provider);

        Assert.Single(store.Files);
    }

    // The rollback: the mode stops new claims while the store stays registered so consumers can
    // keep draining the pointers already on the queue.
    [Fact]
    public async Task NeverMode_LeavesTheDecoratorOut_SoAnOversizedPayloadStaysInline()
    {
        var provider = Services(o => o.ClaimCheckMode = ClaimCheckMode.Never, withClaimStore: true);

        var store = await RunAsync(provider);

        Assert.Empty(store.Files);
    }

    [Fact]
    public async Task NeverMode_SaysSo_SoARollbackIsVisibleToAnOperator()
    {
        var log = new List<string>();
        var provider = Services(o => o.ClaimCheckMode = ClaimCheckMode.Never, withClaimStore: true, log);

        await RunAsync(provider);

        Assert.Contains(log, m => m.Contains("claim check mode Never", StringComparison.Ordinal));
    }

    // Never is the documented rollback, so it has to stop new claims whatever configured them. A
    // pipeline that called WithClaimCheck itself, against a store the host never registered, is the
    // case that would otherwise keep claim-checking while its configuration said it had stopped.
    [Fact]
    public async Task NeverMode_AlsoClearsAClaimCheckThePipelineConfiguredItself()
    {
        var ownStore = new InMemoryFileAdapter();
        var provider = Services(o => o.ClaimCheckMode = ClaimCheckMode.Never, withClaimStore: false,
            pipeline: (b, p) => PipelineWithOwnClaimCheck(ownStore, b, p));

        await RunAsync(provider);

        Assert.Empty(ownStore.Files);
    }

    // The control: without Never, the pipeline's own claim check is left alone.
    [Fact]
    public async Task APipelinesOwnClaimCheck_IsLeftAlone_WhenTheModeIsNotNever()
    {
        var ownStore = new InMemoryFileAdapter();
        var provider = Services(o => o.ClaimCheckThresholdBytes = 1_000, withClaimStore: false,
            pipeline: (b, p) => PipelineWithOwnClaimCheck(ownStore, b, p));

        await RunAsync(provider);

        Assert.Single(ownStore.Files);
    }

    // Registering a store is what makes claim-checking possible at all; the mode only decides
    // whether it is used. Without one, nothing is claim-checked however the mode reads.
    [Theory]
    [InlineData(ClaimCheckMode.Auto)]
    [InlineData(ClaimCheckMode.Always)]
    [InlineData(ClaimCheckMode.Never)]
    public async Task WithoutAClaimStore_NothingIsClaimChecked_WhateverTheMode(ClaimCheckMode mode)
    {
        var provider = Services(o =>
        {
            o.ClaimCheckMode = mode;
            o.ClaimCheckThresholdBytes = 1_000;
        }, withClaimStore: false);

        var store = await RunAsync(provider);

        Assert.Empty(store.Files);
    }

    // The threshold is the broker's message size, which the framework cannot know. In Auto with a
    // store there is no safe default, so composition fails rather than guessing.
    [Fact]
    public void AStoreInAutoModeWithNoThreshold_FailsWhenThePipelineIsComposed()
    {
        var provider = Services(configure: null, withClaimStore: true);
        using var scope = provider.CreateScope();

        Assert.ThrowsAny<Exception>(() =>
            scope.ServiceProvider.GetRequiredService<Extractor<Order, Order, Context>>());
    }

    // The control for the test above: Never needs no threshold, so the same registration composes.
    [Fact]
    public void AStoreInNeverMode_NeedsNoThreshold_SoThePipelineStillComposes()
    {
        var provider = Services(o => o.ClaimCheckMode = ClaimCheckMode.Never, withClaimStore: true);
        using var scope = provider.CreateScope();

        Assert.NotNull(scope.ServiceProvider.GetRequiredService<Extractor<Order, Order, Context>>());
    }

    [Fact]
    public void ANonPositiveThreshold_IsRejectedAtRegistration()
    {
        Assert.Throws<ArgumentException>(() =>
            Services(o => o.ClaimCheckThresholdBytes = -1, withClaimStore: true));
    }

    // The settings are a deployment decision, so a section has to be able to carry them — that is
    // what makes the Never rollback a configuration change rather than a redeploy.
    [Fact]
    public void Configuration_SuppliesTheModeAndThreshold()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Extractor:ClaimCheckMode"] = "Always",
            ["Extractor:ClaimCheckThresholdBytes"] = "230000",
        }).Build();
        var options = new ExtractorOptions().Bind(configuration.GetSection("Extractor"));

        Assert.Equal(ClaimCheckMode.Always, options.ClaimCheckMode);
        Assert.Equal(230_000, options.ClaimCheckThresholdBytes);
    }

    private sealed class CapturingLoggerFactory(List<string> log) : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => new CapturingLogger(log);
        public void AddProvider(ILoggerProvider provider) { }
        public void Dispose() { }
    }

    private sealed class CapturingLogger(List<string> log) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => log.Add(formatter(state, exception));
    }

    private sealed class OrderDeserializer : DeserializeStep<Order, Context>
    {
        public override Task<(BusinessStepResult<Order> Result, Context Context)> ExecuteAsync(
            ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<Order>, Context)>(
                (new BusinessStepResult<Order>.Success(JsonSerializer.Deserialize<Order>(input.Span)!), context));
    }

    private sealed class PassingValidator : ValidateStep<Order, Context>
    {
        public override Task<(BusinessStepResult<Order> Result, Context Context)> ExecuteAsync(
            Order input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<Order>, Context)>(
                (new BusinessStepResult<Order>.Success(input), context));
    }

    private sealed class IdentityTransformer : TransformStep<Order, Order, Context>
    {
        public override Task<(TechnicalStepResult<Order> Result, Context Context)> ExecuteAsync(
            Order input, Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<Order>, Context)>(
                (new TechnicalStepResult<Order>.Success(input), context));
    }
}
