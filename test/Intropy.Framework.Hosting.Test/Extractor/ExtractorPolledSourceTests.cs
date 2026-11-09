using System.Text;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.StatefulSweeps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// A polled source port through the real extractor composition: a port whose source is an
/// <see cref="IPolledSource"/> rather than a file adapter is read once per run, its read goes through
/// the same pipeline as a file, the idempotency key the sweep sets reaches the pipeline's context, and
/// the cursor moves only once the message is published.
/// </summary>
public class ExtractorPolledSourceTests
{
    [Fact]
    public async Task APolledPort_IsReadOnce_Published_AndItsCursorCommitted()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        var store = new Store();
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
        var store = new Store();
        var services = PolledServices(fakes, store, [], ExtractorTestProcess.ValidInput);

        var summary = await Job(services).ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Failed);
        Assert.Equal(new CursorState("w0", "w1"), store.State.Single().Value);
    }

    [Fact]
    public async Task APortWithBothAFileAdapterAndAPolledSource_FailsTheRun()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        var services = fakes.CreateServices();
        services.AddKeyedSingleton<IPolledSource>(ExtractorSweepTestProcess.SourceKey, new Source(""));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Job(services).ExecuteAsync(CancellationToken.None));

        Assert.Contains("both an IFileAdapter and an IPolledSource", error.Message, StringComparison.Ordinal);
    }

    private static ServiceCollection PolledServices(ExtractorSweepTestProcess.SweepFakes fakes, Store store,
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
        services.AddKeyedSingleton<IPolledSource>(ExtractorSweepTestProcess.SourceKey, new Source(content));
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

        public Task<PolledRead?> ReadAsync(CursorRange range, CancellationToken ct) =>
            Task.FromResult<PolledRead?>(new PolledRead(Encoding.UTF8.GetBytes(content), "w1")
            {
                Metadata = new Dictionary<string, string> { ["rows"] = "1" }
            });
    }

    private sealed class Store : ICursorStore
    {
        public Dictionary<string, CursorState> State { get; } = [];

        public Task<CursorState?> GetAsync(string key, CancellationToken ct) => Task.FromResult(State.GetValueOrDefault(key));

        public Task SaveAsync(string key, CursorState state, CancellationToken ct)
        {
            State[key] = state;
            return Task.CompletedTask;
        }
    }
}
