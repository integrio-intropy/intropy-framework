using System.Text;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Testing.Adapters;
using Intropy.Framework.Testing.Services;
using Intropy.Framework.Testing.Topics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// Scope and context isolation across files: one processing scope per file, scoped
/// dependencies disposed after each file regardless of outcome, context metadata never
/// leaks, derived context values survive the pipeline, incident identity is available
/// before deserialization succeeds, the source is selected through the declared DI key,
/// and all four external edges remain replaceable through ordinary DI.
/// </summary>
public class ExtractorJobIsolationTests
{
    private const string ValidInput =
        """{"Id":"input-42","Version":"2026-01-15T09:30:00+00:00","Valid":true}""";

    [Fact]
    public async Task ExecuteAsync_UsesOneScopePerFile_AndDisposesItOnSuccess()
    {
        var (sweep, collector) = CreateProbedSweep("order-1.json", "order-2.json");

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(2, summary.Processed);
        Assert.Equal(2, collector.Entries.Count);
        Assert.Equal(2, collector.Entries.Select(e => e.ScopeId).Distinct().Count()); // one scope per file
        // Every file's scope was disposed. (The pre-batch composition validation also creates
        // and disposes one throwaway scope, so a superset of disposed ids is expected.)
        Assert.All(collector.Entries, e => Assert.Contains(e.ScopeId, collector.DisposedScopeIds));
        Assert.Equal(collector.Entries.Select(e => e.ScopeId).Order().ToList(),
            collector.DisposedScopeIds.Where(id => collector.Entries.Any(e => e.ScopeId == id)).Order().ToList());
    }

    [Fact]
    public async Task ExecuteAsync_DisposesScopedDependenciesOfFailedFilesToo()
    {
        var (sweep, fakes, collector) = CreateProbedSweepStateful("corrupt.json", "order.json");
        fakes.Files.AddUnreadableFile("corrupt.json"); // corrupt.json: read failure → Failed

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Equal(1, summary.Failed);
        // Both real file scopes — success and failure — were disposed.
        Assert.All(collector.Entries, e => Assert.Contains(e.ScopeId, collector.DisposedScopeIds));
    }

    [Fact]
    public async Task ExecuteAsync_ContextMetadataDoesNotLeakBetweenFiles()
    {
        var (sweep, collector) = CreateProbedSweep("order-1.json", "order-2.json");

        await sweep.ExecuteAsync(CancellationToken.None);

        var first = collector.Entries[0];
        var second = collector.Entries[1];
        Assert.Equal("caller-order-1.json", first.Caller); // factory metadata arrived...
        Assert.Equal("caller-order-2.json", second.Caller); // ...preserved, never another file's value
        Assert.Equal("order-1.json", first.FileName); // framework key written per file
        Assert.Equal("order-2.json", second.FileName);
        Assert.NotEqual(first.ScopeId, second.ScopeId); // separate scopes saw their own metadata
    }

    [Fact]
    public async Task ExecuteAsync_DerivedContextValuesSurviveTheFullPipeline()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var derivedTopic = new FakeTopic<ExtractorSweepTestProcess.DerivedContext>();
        var definition = ExtractorSweepTestProcess.DerivedDefinition();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IFileAdapter>(ExtractorSweepTestProcess.SourceKey, fakes.Files);
        services.AddSingleton(derivedTopic);
        services.AddSingleton<SendStep<ExtractorSweepTestProcess.DerivedContext>>(derivedTopic);
        services.AddSingleton(new FakeIdempotencyServiceClient());
        services.AddSingleton<IIdempotencyServiceClient>(p => p.GetRequiredService<FakeIdempotencyServiceClient>());
        services.AddSingleton(new FakeBusinessIncidentServiceClient());
        services.AddSingleton<IBusinessIncidentServiceClient>(p => p.GetRequiredService<FakeBusinessIncidentServiceClient>());
        services.AddTestIdentity().AddExtractor(definition);
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        var sweep = definition.CreateJob(provider, NullLoggerFactory.Instance);

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        var published = Assert.Single(derivedTopic.Events);
        Assert.Equal("owned-by-owner-order-1.json", published.Subject); // derived value survived deserialize → transform → publish
        Assert.Empty(fakes.Files.Files);
    }

    [Fact]
    public async Task ExecuteAsync_IncidentIdentityIsAvailableBeforeDeserializationSucceeds()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("poisoned.json", "not-json");
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        // Deserialization fails, yet the incident carries the source file identity the
        // framework wrote into the context before any step ran.
        var incident = Assert.Single(fakes.Incidents.Incidents);
        Assert.Equal("poisoned.json", incident.Id);
        Assert.Equal("file:poisoned.json", incident.Subject);
        Assert.Equal(1, summary.Processed); // successfully routed: consumed
        Assert.Empty(fakes.Files.Files);
    }

    [Fact]
    public async Task ExecuteAsync_SelectsTheSourceAdapterThroughTheDeclaredKey()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var other = new InMemoryFileAdapter();
        await other.WriteAsync("order-1.json", "wrong-source", Encoding.UTF8);
        var services = fakes.CreateServices();
        services.AddKeyedSingleton<IFileAdapter>("other", other);
        var sweep = CreateSweep(services);

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        // The pipeline content came from the definition's 'source' adapter only; the
        // undeclared 'other' adapter was never consulted and its copy still exists.
        Assert.Equal(1, summary.Processed);
        var published = Assert.Single(fakes.Topic.Events);
        Assert.Equal("event-input-42", published.Subject);
        Assert.Single(other.Files);
    }

    [Fact]
    public async Task AllFourExternalEdges_RemainReplaceable_ForSweepProcessing()
    {
        var replacementFiles = new InMemoryFileAdapter();
        await replacementFiles.WriteAsync("replaced.json", "not-json", Encoding.UTF8);
        var topic = new FakeTopic<ExtractorTestProcess.TestContext>();
        var idempotency = new FakeIdempotencyServiceClient();
        var incidents = new FakeBusinessIncidentServiceClient();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddScoped<ExtractorTestProcess.ScopedDependency>();
        services.AddKeyedSingleton<IFileAdapter>(ExtractorSweepTestProcess.SourceKey, replacementFiles);
        services.AddSingleton(topic);
        services.AddSingleton<SendStep<ExtractorTestProcess.TestContext>>(topic);
        services.AddSingleton(idempotency);
        services.AddSingleton<IIdempotencyServiceClient>(idempotency);
        services.AddSingleton(incidents);
        services.AddSingleton<IBusinessIncidentServiceClient>(incidents);
        var definition = ExtractorSweepTestProcess.Definition();
        services.AddTestIdentity().AddExtractor(definition);
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        var sweep = definition.CreateJob(provider, NullLoggerFactory.Instance);

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        // Every external came from the replacement: the source was the replacement adapter,
        // the incident route hit the replacement incident client, nothing published or committed.
        var incident = Assert.Single(incidents.Incidents);
        Assert.Equal("replaced.json", incident.Id);
        Assert.Empty(topic.Events);
        Assert.Empty(idempotency.Committed);
        Assert.Equal(1, summary.Processed);
        Assert.Empty(replacementFiles.Files);
    }

    [Fact]
    public void DerivedContext_CannotBeConstructedWithoutArguments()
    {
        // Documents the contract under test: the sweep supports contexts with required-argument
        // constructors because construction happens only through ContextFactory.
        Assert.All(typeof(ExtractorSweepTestProcess.DerivedContext).GetConstructors(),
            c => Assert.NotEmpty(c.GetParameters()));
    }

    private static (ExtractorJob<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext> Sweep,
        ExtractorSweepTestProcess.ProbeCollector Collector) CreateProbedSweep(params string[] files)
    {
        var (sweep, _, collector) = CreateProbedSweepStateful(files);
        return (sweep, collector);
    }

    private static (ExtractorJob<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext> Sweep,
        ExtractorSweepTestProcess.SweepFakes Fakes, ExtractorSweepTestProcess.ProbeCollector Collector)
        CreateProbedSweepStateful(params string[] files)
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        foreach (var file in files)
            fakes.Files.AddFile(file, ValidInput);
        var collector = new ExtractorSweepTestProcess.ProbeCollector();
        var sweep = CreateSweep(fakes.CreateServices(
            definition: ExtractorSweepTestProcess.ProbedDefinition(),
            configure: services =>
            {
                services.AddSingleton(collector);
                services.AddScoped<ExtractorSweepTestProcess.ScopeProbe>();
            }));
        return (sweep, fakes, collector);
    }

    private static ExtractorJob<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>
        CreateSweep(IServiceCollection services)
    {
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        return provider.GetRequiredService<TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>>().CreateJob(provider, NullLoggerFactory.Instance);
    }
}
