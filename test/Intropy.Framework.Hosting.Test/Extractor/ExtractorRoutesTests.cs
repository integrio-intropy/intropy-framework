using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// An extractor with routes: its sources declare the ports it sweeps, its routes which pipeline
/// each port's files go through, and the registration checks that the two agree. Each port's files
/// go through their own route's pipeline, which stamps the port on the published event and keeps
/// its idempotency records per port. A misdeclared extractor is rejected at registration without
/// leaving anything behind.
/// </summary>
public class ExtractorRoutesTests
{
    private readonly IIdempotencyServiceClient _idempotency = Substitute.For<IIdempotencyServiceClient>();
    private readonly RecordingSender _sender = new();
    private readonly InMemoryFileAdapter _orders = new();
    private readonly InMemoryFileAdapter _invoices = new();

    public ExtractorRoutesTests() =>
        _idempotency.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));

    [Fact]
    public async Task Run_SendsEachPortsFilesThroughItsRoute()
    {
        _orders.AddFile("order.csv", "o-1");
        _invoices.AddFile("invoice.xml", "i-1");
        await using var provider = Register(routes => routes
            .On<Order, Order>(["orders-se"], Pipeline<Order>(id => new Order(id), "orders.accepted"))
            .On<Invoice, Invoice>(["invoices-no"], Pipeline<Invoice>(id => new Invoice(id), "invoices.accepted")));

        var summary = await RunAsync(provider);

        Assert.Equal(2, summary.Processed);
        Assert.Collection(_sender.Sent,
            order =>
            {
                Assert.Equal("orders.accepted", order.Type);
                Assert.Equal("orders-se", order[CloudEventExtensionKeys.SourcePort]);
                Assert.Equal(new Order("o-1"), order.Data);
            },
            invoice =>
            {
                Assert.Equal("invoices.accepted", invoice.Type);
                Assert.Equal("invoices-no", invoice[CloudEventExtensionKeys.SourcePort]);
                Assert.Equal(new Invoice("i-1"), invoice.Data);
            });
        Assert.Empty(_orders.Files);
        Assert.Empty(_invoices.Files);
    }

    [Fact]
    public async Task Run_PassesTheFilesBytesToTheDeserializerUndecoded()
    {
        // Latin-1 bytes are not valid UTF-8: they reach the deserializer exactly as stored, so it
        // owns the encoding and nothing is lost to a decode before it.
        var latin1 = System.Text.Encoding.Latin1;
        _orders.AddFile("order.csv", latin1.GetBytes("Zoë"));
        await using var provider = Register(routes => routes
            .On<Order, Order>(["orders-se"], Pipeline<Order>(id => new Order(id), "orders.accepted", latin1))
            .On<Invoice, Invoice>(["invoices-no"], Pipeline<Invoice>(id => new Invoice(id), "invoices.accepted")));

        await RunAsync(provider);

        Assert.Equal(new Order("Zoë"), _sender.Sent.Single().Data);
    }

    [Fact]
    public async Task Run_KeepsIdempotencyRecordsPerPort()
    {
        // The same id on both ports of one route: two records, so neither is a duplicate of the other.
        _orders.AddFile("a.csv", "1");
        _invoices.AddFile("b.csv", "1");
        await using var provider = Register(routes =>
            routes.OnAny<Order, Order>(Pipeline<Order>(id => new Order(id), "orders.accepted")));

        var summary = await RunAsync(provider);

        Assert.Equal(2, summary.Processed);
        await _idempotency.Received(1).GetStatusAsync(Arg.Is<MessageInfo>(m => m.Id == "orders-se:1"));
        await _idempotency.Received(1).GetStatusAsync(Arg.Is<MessageInfo>(m => m.Id == "invoices-no:1"));
        Assert.Equal(["orders-se", "invoices-no"], _sender.Sent.Select(e => e[CloudEventExtensionKeys.SourcePort]));
    }

    [Fact]
    public async Task Run_WhenARouteCannotBeComposed_FailsBeforeAnyFileIsListed()
    {
        _orders.AddFile("order.csv", "o-1");
        await using var provider = Register(routes => routes
            .On<Order, Order>(["orders-se"], Pipeline<Order>(id => new Order(id), "orders.accepted"))
            .On<Invoice, Invoice>(["invoices-no"], (builder, _) => builder));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => RunAsync(provider));

        Assert.Contains("Extractor 'orders-extractor' route 'invoices-no' composition failed", error.Message);
        Assert.Single(_orders.Files);
        Assert.Empty(_sender.Sent);
    }

    [Fact]
    public void AddExtractor_RejectsAPortNoRouteHandles()
    {
        var error = Assert.Throws<InvalidOperationException>(() => GetServices().AddExtractor(Sources,
            routes => routes.On<Order, Order>(["orders-se"], (builder, _) => builder)));

        Assert.Contains("no route handles source port 'invoices-no'", error.Message);
    }

    [Fact]
    public void AddExtractor_RejectsARouteForEveryFileCombinedWithOtherRoutes()
    {
        var error = Assert.Throws<InvalidOperationException>(() => GetServices().AddExtractor(Sources,
            routes => routes
                .On<Order, Order>(["orders-se"], (builder, _) => builder)
                .OnAny<Invoice, Invoice>((builder, _) => builder)));

        Assert.Contains("cannot be combined with other routes", error.Message);
    }

    [Fact]
    public void AddExtractor_RejectsNoRoutes()
    {
        var error = Assert.Throws<InvalidOperationException>(() => GetServices().AddExtractor(Sources, _ => { }));

        Assert.Contains("Extractor composition failed: no route is declared", error.Message);
    }

    [Fact]
    public void On_RejectsNoPorts() =>
        Assert.Throws<ArgumentException>(() =>
            new ExtractorRoutes<Context>().On<Order, Order>([], (builder, _) => builder));

    [Fact]
    public void AddExtractor_RejectsAnAlreadyRegisteredSourcePort()
    {
        var services = GetServices();
        services.AddSourcePort("orders-se");

        var error = Assert.Throws<InvalidOperationException>(() => services.AddExtractor(
            sources => sources.Port("invoices-no"), routes => routes.OnAny<Order, Order>((builder, _) => builder)));

        Assert.Contains("already registered", error.Message);
    }

    [Fact]
    public void AddExtractor_WhenRejected_LeavesNothingBehind()
    {
        // A corrected retry registers: the rejected attempt left no marker and no port.
        var services = GetServices();
        Assert.Throws<InvalidOperationException>(() => services.AddExtractor(Sources,
            routes => routes.On<Order, Order>(["orders-se"], (builder, _) => builder)));

        services.AddExtractor(Sources, routes => routes.OnAny<Order, Order>((builder, _) => builder));

        Assert.Equal(2, services.BuildServiceProvider().GetServices<SourcePort>().Count());
    }

    [Fact]
    public void AddExtractor_RejectsASecondExtractor()
    {
        var services = GetServices();
        services.AddExtractor(Sources, routes => routes.OnAny<Order, Order>((builder, _) => builder));

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.AddExtractor<Order, Order>((builder, _) => builder));

        Assert.Contains("extractor", error.Message);
    }

    private static void Sources(FileSources sources) => sources.Port("orders-se").Port("invoices-no");

    /// <summary>A complete pipeline that reads the file's text as the record id and publishes the
    /// record as <paramref name="type"/>.</summary>
    private static Func<ExtractorBuilder<T, T, Context>, IServiceProvider, ExtractorBuilder<T, T, Context>>
        Pipeline<T>(Func<string, T> read, string type, System.Text.Encoding? encoding = null) where T : IRecord =>
        (builder, _) => builder
            .WithDeserializer(new ReadDeserializer<T>(read, encoding ?? System.Text.Encoding.UTF8))
            .WithValidator(new AcceptValidator<T>())
            .WithIdempotency((record, _) => record.Id, (_, _) => DateTimeOffset.UnixEpoch)
            .WithTransformer(new IdentityTransformer<T>())
            .WithSerializer(new CloudEventSerializeStep<T, Context>(record => record.Id, _ => DateTimeOffset.UnixEpoch,
                type: (_, _) => type))
            .WithSenderFromServices()
            .WithBusinessIncidents(_ => "unknown", _ => "unknown");

    private ServiceProvider Register(Action<ExtractorRoutes<Context>> routes)
    {
        var services = GetServices();
        services.AddSingleton(_idempotency);
        services.AddSingleton(Substitute.For<IBusinessIncidentServiceClient>());
        services.AddSingleton<SendStep<Context>>(_sender);
        services.AddKeyedSingleton<IFileAdapter>("orders-se", _orders);
        services.AddKeyedSingleton<IFileAdapter>("invoices-no", _invoices);
        services.AddExtractor(Sources, routes);
        return services.BuildServiceProvider();
    }

    private static Task<RunSummary> RunAsync(IServiceProvider provider) =>
        provider.GetRequiredService<ExtractorJob<Context>>().ExecuteAsync(CancellationToken.None);

    private static ServiceCollection GetServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "orders-extractor";
            options.ServiceNamespace = "example";
        });
        return services;
    }

    public interface IRecord
    {
        string Id { get; }
    }

    public sealed record Order(string Id) : IRecord;

    public sealed record Invoice(string Id) : IRecord;

    private sealed class ReadDeserializer<T>(Func<string, T> read, System.Text.Encoding encoding) : DeserializeStep<T, Context>
    {
        public override string StepName => "Deserialize";

        public override Task<(BusinessStepResult<T> Result, Context Context)> ExecuteAsync(ReadOnlyMemory<byte> input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<T>, Context)>((new BusinessStepResult<T>.Success(read(encoding.GetString(input.Span))), context));
    }

    private sealed class AcceptValidator<T> : ValidateStep<T, Context>
    {
        public override string StepName => "Validate";

        public override Task<(BusinessStepResult<T> Result, Context Context)> ExecuteAsync(T input, Context context,
            CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<T>, Context)>((new BusinessStepResult<T>.Success(input), context));
    }

    private sealed class IdentityTransformer<T> : TransformStep<T, T, Context>
    {
        public override string StepName => "Transform";

        public override Task<(TechnicalStepResult<T> Result, Context Context)> ExecuteAsync(T input, Context context,
            CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<T>, Context)>((new TechnicalStepResult<T>.Success(input), context));
    }

    private sealed class RecordingSender : SendStep<Context>
    {
        public List<CloudEvent> Sent { get; } = [];

        public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(CloudEvent input,
            Context context, CancellationToken ct)
        {
            Sent.Add(input);
            return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
                (new TechnicalStepResult<CloudEvent>.Success(input), context));
        }
    }
}
