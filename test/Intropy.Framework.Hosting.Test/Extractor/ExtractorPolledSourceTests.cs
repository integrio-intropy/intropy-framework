using Intropy.Framework.Testing.StatefulSweeps;
using System.Text;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.StatefulSweeps;
using Intropy.Framework.Hosting.Sweeps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// A polled source port through the real extractor composition: a port declared as a
/// <see cref="PolledSourcePort"/> is read once per run from its <see cref="IPolledSource"/>, its read goes through
/// the same pipeline as a file, the idempotency key the sweep sets reaches the pipeline's context, and
/// the cursor moves only once the message is published.
/// </summary>
public class ExtractorPolledSourceTests
{
    [Fact]
    public async Task APolledPort_IsReadOnce_Published_AndItsCursorCommitted()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        var store = new InMemoryCursorStore();
        var metadata = new List<Dictionary<string, string>>();
        var services = PolledServices(fakes, store, metadata, ExtractorTestProcess.ValidInput);

        var summary = await Job(services).ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Single(fakes.Topic.Events);
        Assert.Equal(new CursorState("w1", null), store.State.Single().Value);
        // The pipeline was keyed on the range the sweep pinned, not on the payload.
        Assert.Equal("source:w0:w1", Assert.Single(fakes.Idempotency.Committed).Id);
        var context = Assert.Single(metadata);
        Assert.Equal("source", context[SourceContextKeys.SourcePort]);
        Assert.True(context.ContainsKey(SourceContextKeys.ReceivedAt));
        Assert.DoesNotContain(SourceContextKeys.FileName, context.Keys);
        Assert.Equal("1", context["rows"]);
    }

    [Fact]
    public async Task APolledPort_WhosePublishFails_KeepsItsRangePending()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Topic.SendException = new InvalidOperationException("Broker unreachable");
        var store = new InMemoryCursorStore();
        var services = PolledServices(fakes, store, [], ExtractorTestProcess.ValidInput);

        var summary = await Job(services).ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Failed);
        Assert.Equal(new CursorState("w0", "w1"), store.State.Single().Value);
    }

    [Fact]
    public async Task APolledPort_DeclaredWithoutItsSource_FailsTheRunBeforeReading()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        var services = fakes.CreateServices();
        services.Remove(services.Single(d => d.ServiceType == typeof(SourcePort)));
        services.AddSourcePort(new PolledSourcePort(ExtractorSweepTestProcess.SourceKey));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Job(services).ExecuteAsync(CancellationToken.None));

        Assert.Contains("no IPolledSource is registered", error.Message, StringComparison.Ordinal);
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task APolledSource_WhosePortIsDeclaredByNameAlone_FailsTheRunNamingTheFix()
    {
        // The trap: a port declared by name (Port("x"), SourcePort = "x") is a file port, whatever
        // source is registered for it.
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        var services = fakes.CreateServices();
        services.AddPolledSource(ExtractorSweepTestProcess.SourceKey, _ => new Source(ExtractorTestProcess.ValidInput));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Job(services).ExecuteAsync(CancellationToken.None));

        Assert.Contains("is declared as a file port, but a polled source (IPolledSource) is registered", error.Message,
            StringComparison.Ordinal);
        Assert.Contains($"new PolledSourcePort(\"{ExtractorSweepTestProcess.SourceKey}\")", error.Message, StringComparison.Ordinal);
        Assert.Empty(fakes.Topic.Events);
    }

    private static ServiceCollection PolledServices(ExtractorSweepTestProcess.SweepFakes fakes, InMemoryCursorStore store,
        List<Dictionary<string, string>> metadata, string content)
    {
        var services = fakes.CreateServices(definition: ExtractorSweepTestProcess.Definition() with
        {
            InputIdentity = (_, context) => context.Metadata[SourceContextKeys.IdempotencyKey],
            ContextFactory = (seeded, _) =>
            {
                metadata.Add(new Dictionary<string, string>(seeded));
                return new ExtractorTestProcess.TestContext();
            }
        });
        var file = services.Single(d => d.ServiceType == typeof(IFileAdapter)
                                        && Equals(d.ServiceKey, ExtractorSweepTestProcess.SourceKey));
        services.Remove(file);
        // The port is a polled one: replace the file port the test extractor declares.
        services.Remove(services.Single(d => d.ServiceType == typeof(SourcePort)));
        services.AddPolledSourcePort(ExtractorSweepTestProcess.SourceKey, _ => new Source(content));
        services.AddKeyedSingleton<ICursorStore>("watermarks", store);
        return services;
    }

    private static Hosting.Extractor.ExtractorJob<ExtractorTestProcess.TestContext> Job(ServiceCollection services)
    {
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        return provider.GetRequiredService<TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
            ExtractorTestProcess.TestContext>>().CreateJob(provider, NullLoggerFactory.Instance);
    }

    private sealed class Source(string content) : IPolledSource
    {
        public CursorOptions? Cursor { get; } = new("watermarks", "w0");

        public Task<PolledResult> ReadAsync(CursorRange range, CancellationToken ct) =>
            Task.FromResult<PolledResult>(PolledResult.Page([new PolledItem(Encoding.UTF8.GetBytes(content))
            {
                Metadata = new Dictionary<string, string> { ["rows"] = "1" }
            }], "w1"));
    }
}
