using CloudNative.CloudEvents;
using Dapr.Client;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.Common;

/// <summary>
/// The archive as the hosting layer wires it: a registered archive store and the component's
/// <c>Archive</c> settings decide whether, and when, the pipelines the framework composes archive.
/// Driven through a real sweep, because what matters is what happens to the source file: an
/// incident's file is completed only once its payload is in the archive.
/// </summary>
public class ArchiveStoreTests
{
    private const string Port = "orders-se";

    private readonly InMemoryFileAdapter _source = new();
    private readonly InMemoryFileAdapter _archive = new();
    private readonly IBusinessIncidentServiceClient _incidents = Substitute.For<IBusinessIncidentServiceClient>();

    [Fact]
    public void AddArchiveStore_ReadsBothSections_AndRegistersTheAdapterKeyed()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ArchiveStore:Kind"] = "Local",
            ["ArchiveStore:DaprBindingName"] = "archive-store",
            ["Archive:Mode"] = "OnReceipt",
            ["Archive:BrowseBaseUrl"] = "https://starchive.blob.core.windows.net/archive/"
        }).Build();
        var services = new ServiceCollection().AddSingleton(Substitute.For<DaprClient>());

        services.AddArchiveStore(configuration);

        using var provider = services.BuildServiceProvider();
        Assert.IsType<LocalFileAdapter>(
            provider.GetKeyedService<IFileAdapter>(ArchiveStoreServiceCollectionExtensions.ArchiveStoreKey));
        var options = provider.GetRequiredService<ArchiveOptions>();
        Assert.Equal(ArchiveMode.OnReceipt, options.Mode);
        Assert.Equal(new Uri("https://starchive.blob.core.windows.net/archive/"), options.BrowseBaseUrl);
    }

    [Fact]
    public void ArchiveOptions_DefaultToOnIncident()
    {
        var options = new ArchiveOptions().Bind(new ConfigurationBuilder().Build());

        Assert.Equal(ArchiveMode.OnIncident, options.Mode);
        Assert.Null(options.BrowseBaseUrl);
    }

    [Fact]
    public void ArchiveOptions_RefuseARelativeBrowseBaseUrl()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["BrowseBaseUrl"] = "archive/"
        }).Build();

        var error = Assert.Throws<InvalidOperationException>(() => new ArchiveOptions().Bind(configuration));

        Assert.Contains(nameof(ArchiveOptions.BrowseBaseUrl), error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnIncidentsFile_IsArchived_BeforeTheSweepDeletesIt()
    {
        _source.AddFile("bad order.xml", "reject");
        await using var provider = Register(new ArchiveOptions());

        var summary = await RunAsync(provider);

        Assert.Equal(1, summary.Processed);
        Assert.Empty(_source.Files);
        var (key, content) = Assert.Single(_archive.Files);
        Assert.Matches("^orders-extractor/[0-9]{4}/[0-9]{2}/[0-9]{2}/bad_order\\.xml(_[0-9a-f]{32})?$", key);
        Assert.Equal("reject", System.Text.Encoding.UTF8.GetString(content));
        Assert.Equal(key, RoutedIncident()!.Context![ArchiveContextKeys.IncidentDataRef]);
    }

    [Fact]
    public async Task APublishedFile_IsNotArchived_OnIncident()
    {
        _source.AddFile("order.xml", "accept");
        await using var provider = Register(new ArchiveOptions());

        await RunAsync(provider);

        Assert.Empty(_source.Files);
        Assert.Empty(_archive.Files);
    }

    // The payload is never let go of without its copy: no incident, and the file stays for the next run.
    [Fact]
    public async Task AFailedArchiveWrite_KeepsTheIncidentsFile_AndRaisesNoIncident()
    {
        _source.AddFile("bad.xml", "reject");
        _archive.WriteException = new IOException("archive unavailable");
        await using var provider = Register(new ArchiveOptions());

        var summary = await RunAsync(provider);

        Assert.Equal(1, summary.Failed);
        Assert.Single(_source.Files);
        Assert.Null(RoutedIncident());
    }

    [Fact]
    public async Task OnReceiptMode_ArchivesAPublishedFile()
    {
        _source.AddFile("order.xml", "accept");
        await using var provider = Register(new ArchiveOptions { Mode = ArchiveMode.OnReceipt });

        await RunAsync(provider);

        Assert.Single(_archive.Files);
    }

    // A store with the mode set to Never archives nothing, so archiving can be switched off without
    // removing the binding.
    [Fact]
    public async Task NeverMode_ArchivesNothing()
    {
        _source.AddFile("bad.xml", "reject");
        await using var provider = Register(new ArchiveOptions { Mode = ArchiveMode.Never });

        await RunAsync(provider);

        Assert.Empty(_archive.Files);
        Assert.False(RoutedIncident()!.Context?.ContainsKey(ArchiveContextKeys.IncidentDataRef) ?? false);
    }

    private ServiceProvider Register(ArchiveOptions options)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddIntropyFramework(o =>
        {
            o.ComponentName = "orders-extractor";
            o.ServiceNamespace = "example";
        });
        services.AddSingleton(_incidents);
        services.AddSingleton<SendStep<Context>>(new DiscardingSender());
        services.AddKeyedSingleton<IFileAdapter>(Port, _source);
        // What AddArchiveStore registers, with the store swapped for one the test can read.
        services.AddKeyedSingleton<IFileAdapter>(ArchiveStoreServiceCollectionExtensions.ArchiveStoreKey, _archive);
        services.AddSingleton(options);
        services.AddExtractor(sources => sources.Port(Port),
            routes => routes.On<string, string>([Port], (builder, _) => builder
                .WithDeserializer(new TextDeserializer())
                .WithValidator(new RejectingValidator())
                .WithTransformer(new IdentityTransformer())
                .WithSerializer(new CloudEventSerializeStep<string, Context>(s => s, _ => DateTimeOffset.UnixEpoch))
                .WithSenderFromServices()
                .WithBusinessIncidents(c => c.Metadata[SourceContextKeys.FileName], _ => "orders")));
        return services.BuildServiceProvider();
    }

    private static Task<RunSummary> RunAsync(IServiceProvider provider) =>
        provider.GetRequiredService<ExtractorJob<Context>>().ExecuteAsync(CancellationToken.None);

    private BusinessIncidentData? RoutedIncident() =>
        _incidents.ReceivedCalls()
            .SingleOrDefault(c => c.GetMethodInfo().Name == nameof(IBusinessIncidentServiceClient.Trigger))
            ?.GetArguments()[3] as BusinessIncidentData;

    private sealed class TextDeserializer : DeserializeStep<string, Context>
    {
        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
            ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>(
                (new BusinessStepResult<string>.Success(System.Text.Encoding.UTF8.GetString(input.Span)), context));
    }

    private sealed class RejectingValidator : ValidateStep<string, Context>
    {
        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
            string input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>(input == "reject"
                ? (new BusinessStepResult<string>.Failure(new BusinessIncidentData { Description = "rejected" },
                    Retriable: false), context)
                : (new BusinessStepResult<string>.Success(input), context));
    }

    private sealed class IdentityTransformer : TransformStep<string, string, Context>
    {
        public override Task<(TechnicalStepResult<string> Result, Context Context)> ExecuteAsync(
            string input, Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<string>, Context)>(
                (new TechnicalStepResult<string>.Success(input), context));
    }

    private sealed class DiscardingSender : SendStep<Context>
    {
        public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(
            CloudEvent input, Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
                (new TechnicalStepResult<CloudEvent>.Success(input), context));
    }
}
