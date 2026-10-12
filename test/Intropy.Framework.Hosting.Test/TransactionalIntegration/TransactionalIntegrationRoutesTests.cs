using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

/// <summary>
/// A Transactional Integration with send routes: its sources declare the ports it sweeps, its routes
/// which send pipeline each port's messages go through, and the registration checks that the two
/// agree. The receive side tags each item with its port. A misdeclared integration is rejected at
/// registration without leaving anything behind.
/// </summary>
public class TransactionalIntegrationRoutesTests
{
    private static readonly TransactionalIntegrationDefinition<Context> Definition = new();

    [Fact]
    public void Port_RejectsAnEmptyPort() =>
        Assert.Throws<ArgumentException>(() => new FileSources().Port(" "));

    [Fact]
    public void Port_RejectsADuplicatePort()
    {
        var sources = new FileSources().Port("orders-in");

        var error = Assert.Throws<ArgumentException>(() => sources.Port("orders-in"));

        Assert.Contains("'orders-in' is already declared", error.Message);
    }

    [Fact]
    public void On_RejectsAMatchWithoutAPort()
    {
        var error = Assert.Throws<ArgumentException>(() =>
            new TransactionalIntegrationRoutes<Context>().On<OrderFile, string>(m => m, Send));

        Assert.Contains("use OnAny", error.Message);
    }

    [Fact]
    public void Match_RejectsAnEmptyPortList()
    {
        Assert.Throws<ArgumentException>(() => new FileMatch().Port());
        Assert.Throws<ArgumentException>(() => new FileMatch().Port(" "));
    }

    [Fact]
    public void AddTransactionalIntegration_RegistersEachSourcePortWithItsCompletion()
    {
        var services = GetServices();

        services.AddTransactionalIntegration(Definition,
            sources => sources.Port("orders-in").Port("invoices-in", FileCompletion.Archive("processed")),
            routes => routes.OnAny<OrderFile, string>(Send));

        var ports = services.BuildServiceProvider().GetServices<SourcePort>().ToList();
        Assert.Equal(["orders-in", "invoices-in"], ports.Select(p => p.Name));
        Assert.Same(FileCompletion.Delete, ports[0].Completion);
        Assert.NotSame(FileCompletion.Delete, ports[1].Completion);
    }

    [Fact]
    public async Task AddTransactionalIntegration_ReceivesFromEveryPort_TaggingEachItemWithItsPort()
    {
        var services = GetServices();
        services.AddKeyedSingleton<IFileAdapter>("orders-in", new InMemoryFileAdapter().AddFile("order-1.csv", "o"));
        services.AddKeyedSingleton<IFileAdapter>("invoices-in", new InMemoryFileAdapter().AddFile("invoice-1.xml", "i"));
        var received = new List<SourceItem>();
        var receivePipeline = Substitute.For<IReceivePipeline<Context>>();
        receivePipeline.Execute(Arg.Do<SourceItem>(received.Add), Arg.Any<Context>(), Arg.Any<bool>(),
                Arg.Any<CancellationToken>())
            .Returns(call => (new StepResult<SourceItem>.Success(call.Arg<SourceItem>()), call.Arg<Context>()));
        services.AddSingleton(receivePipeline);
        services.AddTransactionalIntegration(Definition,
            sources => sources.Port("orders-in").Port("invoices-in"),
            routes => routes
                .On<OrderFile, string>(m => m.Port("orders-in"), Send)
                .On<InvoiceFile, string>(m => m.Port("invoices-in"), Send));

        var summary = await services.BuildServiceProvider()
            .GetRequiredService<TransactionalIntegrationReceiver<Context>>().SweepAsync(CancellationToken.None);

        Assert.Equal([("order-1.csv", "orders-in"), ("invoice-1.xml", "invoices-in")],
            received.Select(item => (item.Id, item.Port)));
        Assert.Equal(new RunSummary(Processed: 2, Failed: 0, Skipped: 0), summary);
    }

    [Fact]
    public void AddTransactionalIntegration_BuildsEveryRoutesPipelineAtStartup()
    {
        var services = GetServices();
        services.AddTransactionalIntegration(Definition, sources => sources.Port("orders-in"),
            routes => routes.On<OrderFile, string>(m => m.Port("orders-in"), (send, _) => send));

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.BuildServiceProvider().GetRequiredService<SendSideRun>());

        Assert.Contains("route 'orders-in' composition failed", error.Message);
    }

    [Fact]
    public void AddTransactionalIntegration_WithConfiguration_BindsTheDefinitionAndKeepsTheSources()
    {
        var services = GetServices();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["DaprTopicName"] = "configured-topic" })
            .Build();

        services.AddTransactionalIntegration(configuration, () => new TransactionalIntegrationDefinition<Context>(),
            sources => sources.Port("orders-in"), routes => routes.OnAny<OrderFile, string>(Send));

        var provider = services.BuildServiceProvider();
        Assert.Equal("configured-topic", provider.GetRequiredService<TransactionalIntegrationOptions>().DaprTopicName);
        Assert.Equal("orders-in", Assert.Single(provider.GetServices<SourcePort>()).Name);
        Assert.NotNull(provider.GetRequiredService<SendSideRun>());
    }

    [Fact]
    public void AddTransactionalIntegration_RejectsARouteForEveryFileCombinedWithOtherRoutes()
    {
        var services = GetServices();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(Definition,
            sources => sources.Port("orders-in"),
            routes => routes.On<OrderFile, string>(m => m.Port("orders-in"), Send).OnAny<OrderFile, string>(Send)));

        Assert.Contains("cannot be combined with other routes", error.Message);
    }

    [Fact]
    public void AddTransactionalIntegration_RejectsARouteNamingAnUndeclaredPort()
    {
        var services = GetServices();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(Definition,
            sources => sources.Port("orders-in"),
            routes => routes.On<OrderFile, string>(m => m.Port("orders-in", "stock-in"), Send)));

        Assert.Contains("'stock-in', which is not declared", error.Message);
    }

    [Fact]
    public void AddTransactionalIntegration_RejectsAPortNoRouteHandles()
    {
        var services = GetServices();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(Definition,
            sources => sources.Port("orders-in").Port("invoices-in"),
            routes => routes.On<OrderFile, string>(m => m.Port("orders-in"), Send)));

        Assert.Contains("no route handles source port 'invoices-in'", error.Message);
    }

    [Fact]
    public void AddTransactionalIntegration_RejectsAPortRoutedTwice()
    {
        var services = GetServices();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(Definition,
            sources => sources.Port("orders-in").Port("invoices-in"),
            routes => routes
                .On<OrderFile, string>(m => m.Port("orders-in"), Send)
                .On<InvoiceFile, string>(m => m.Port("invoices-in", "orders-in"), Send)));

        Assert.Contains("'orders-in' is handled by more than one route", error.Message);
    }

    [Fact]
    public void AddTransactionalIntegration_RoutesSeveralPortsThroughOneRoute()
    {
        var services = GetServices();

        services.AddTransactionalIntegration(Definition, sources => sources.Port("orders-se").Port("orders-no"),
            routes => routes.On<OrderFile, string>(m => m.Port("orders-se", "orders-no"), Send));

        Assert.Equal(2, services.BuildServiceProvider().GetServices<SourcePort>().Count());
    }

    [Fact]
    public void AddTransactionalIntegration_RejectsADefinitionThatSetsASourcePortOrCompletion()
    {
        Assert.Contains("SourcePort/Completion", Assert.Throws<InvalidOperationException>(() =>
            GetServices().AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context> { SourcePort = "orders-in" },
                sources => sources.Port("orders-in"), routes => routes.OnAny<OrderFile, string>(Send))).Message);
        Assert.Throws<InvalidOperationException>(() =>
            GetServices().AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context> { Completion = FileCompletion.Delete },
                sources => sources.Port("orders-in"), routes => routes.OnAny<OrderFile, string>(Send)));
    }

    [Fact]
    public void AddTransactionalIntegration_RejectsNoSources()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            GetServices().AddTransactionalIntegration(Definition, _ => { }, routes => routes.OnAny<OrderFile, string>(Send)));

        Assert.Contains("no source port is declared", error.Message);
    }

    [Fact]
    public void AddTransactionalIntegration_RejectsNoRoutes()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            GetServices().AddTransactionalIntegration(Definition, sources => sources.Port("orders-in"), _ => { }));

        Assert.Contains("no route is declared", error.Message);
    }

    [Fact]
    public void AddTransactionalIntegration_RejectsAnAlreadyRegisteredSourcePort()
    {
        var services = GetServices();
        services.AddSourcePort("orders-in");

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(Definition,
            sources => sources.Port("invoices-in"), routes => routes.OnAny<OrderFile, string>(Send)));

        Assert.Contains("already registered", error.Message);
    }

    [Fact]
    public void AddSourcePort_AfterSources_IsRejected()
    {
        var services = GetServices();
        services.AddTransactionalIntegration(Definition, sources => sources.Port("orders-in"),
            routes => routes.OnAny<OrderFile, string>(Send));

        Assert.Throws<InvalidOperationException>(() => services.AddSourcePort("invoices-in"));
    }

    [Fact]
    public void AddTransactionalIntegration_WhenRejected_LeavesNothingBehind()
    {
        // A corrected retry registers: the rejected attempt left no marker and no port.
        var services = GetServices();
        Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(Definition,
            sources => sources.Port("orders-in").Port("invoices-in"),
            routes => routes.On<OrderFile, string>(m => m.Port("orders-in"), Send)));

        services.AddTransactionalIntegration(Definition, sources => sources.Port("orders-in"),
            routes => routes.OnAny<OrderFile, string>(Send));

        Assert.Single(services.BuildServiceProvider().GetServices<SourcePort>());
    }

    /// <summary>A complete send pipeline of substituted steps: the tests check which route a file
    /// gets, not what it sends.</summary>
    private static SendPipelineBuilder<TInput, string, Context> Send<TInput>(
        SendPipelineBuilder<TInput, string, Context> builder, IServiceProvider _) => builder
        .WithDeserializer(Substitute.For<DeserializeStep<TInput, Context>>())
        .WithValidator(Substitute.For<ValidateStep<TInput, Context>>())
        .WithTransformer(Substitute.For<TransformStep<TInput, string, Context>>())
        .WithSerializer(Substitute.For<SerializeStep<string, Context>>())
        .WithSender(Substitute.For<SendStep<Context>>());

    private static ServiceCollection GetServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_ => new DaprClientBuilder().Build());
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "orders-integration";
            options.ServiceNamespace = "example";
        });
        return services;
    }

    public sealed record OrderFile(string Id);

    public sealed record InvoiceFile(string Number);
}
