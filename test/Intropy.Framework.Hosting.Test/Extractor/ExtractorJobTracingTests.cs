using System.Diagnostics;
using System.Text;
using Intropy.Framework.Adapters.Common;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.Extractor;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// Each swept file is its own trace, linked to the job's span: its read, its pipeline and its
/// completion are all in it, failures are named on its root span, and its logs carry the file.
/// </summary>
public class ExtractorJobTracingTests
{
    private const string HostingActivitySource = "Intropy.Framework.Hosting";
    // PipelineTracing (Intropy.Framework.Core) starts the pipeline spans.
    private const string PipelineActivitySource = "Intropy.Framework.Core";
    private const string FileSpanName = $"process {ExtractorSweepTestProcess.SourceKey}";

    private const string ValidInput =
        """{"Id":"input-42","Version":"2026-01-15T09:30:00+00:00","Valid":true}""";

    [Fact]
    public async Task ExecuteAsync_TracesEachFileAsItsOwnTraceLinkedToTheJob()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput.Replace("input-42", "input-43"));
        var sweep = CreateSweep(fakes.CreateServices());

        var (job, files, spans) = await CaptureFileTracesAsync(ct => sweep.ExecuteAsync(ct));

        Assert.Equal(2, files.Count);
        Assert.All(files, file => Assert.NotEqual(job.TraceId, file.TraceId));
        Assert.NotEqual(files[0].TraceId, files[1].TraceId);
        Assert.Equal(["order-1.json", "order-2.json"], files.Select(f => f.GetTagItem("intropy.file.name")).Order());
        Assert.All(files, file =>
        {
            Assert.Equal("consumed", file.GetTagItem("intropy.sweep.outcome"));
            // The component is the resource's service.name, on every span already.
            Assert.Null(file.GetTagItem("intropy.component.name"));
            // The pipeline continues the file's trace instead of starting one of its own.
            var pipeline = Assert.Single(spans, s => s.Source.Name == PipelineActivitySource &&
                s.DisplayName == "Pipeline.orders-extractor.Process" && s.TraceId == file.TraceId);
            Assert.Equal(file.SpanId, pipeline.ParentSpanId);
        });
    }

    [Fact]
    public async Task ExecuteAsync_ReadsAndCompletesEachFileInsideItsOwnTrace()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput.Replace("input-42", "input-43"));
        var adapter = new TraceRecordingFileAdapter(fakes.Files);
        var sweep = CreateSweep(fakes.CreateServices(adapterOverride: adapter));

        var (_, files, _) = await CaptureFileTracesAsync(ct => sweep.ExecuteAsync(ct));

        Assert.All(files, file =>
        {
            var name = (string)file.GetTagItem("intropy.file.name")!;
            Assert.Equal(file.TraceId, adapter.TraceOf("get", name));
            Assert.Equal(file.TraceId, adapter.TraceOf("delete", name));
        });
    }

    [Fact]
    public async Task ExecuteAsync_MarksTheFileSpanAsAnError_WhenCompletionFails()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.SetDeleteException("order-1.json", new InvalidOperationException("Source refuses deletes"));
        var sweep = CreateSweep(fakes.CreateServices());

        var (_, files, _) = await CaptureFileTracesAsync(ct => sweep.ExecuteAsync(ct));

        var file = Assert.Single(files);
        Assert.Equal(ActivityStatusCode.Error, file.Status);
        Assert.Equal("Source refuses deletes", file.StatusDescription);
        Assert.Contains(file.Events, e => e.Name == "exception");
        Assert.Equal("failed", file.GetTagItem("intropy.sweep.outcome"));
    }

    [Fact]
    public async Task ExecuteAsync_NamesTheFailureOnTheFileSpan_WhenTheFileHasNoContent()
    {
        // The failure is returned before any pipeline span exists: the file span must carry it.
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var adapter = new ExtractorSweepTestProcess.EmptyContentFileAdapter(fakes.Files, "order-1.json");
        var sweep = CreateSweep(fakes.CreateServices(adapterOverride: adapter));

        var (_, files, _) = await CaptureFileTracesAsync(ct => sweep.ExecuteAsync(ct));

        var file = Assert.Single(files);
        Assert.Equal(ActivityStatusCode.Error, file.Status);
        Assert.Equal("The file has no content", file.StatusDescription);
    }

    [Fact]
    public async Task ExecuteAsync_RecordsTheExceptionOnTheFileSpan_WhenTheReadFails()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddUnreadableFile("corrupt.json");
        var sweep = CreateSweep(fakes.CreateServices());

        var (_, files, _) = await CaptureFileTracesAsync(ct => sweep.ExecuteAsync(ct));

        var file = Assert.Single(files);
        Assert.Equal(ActivityStatusCode.Error, file.Status);
        Assert.Contains(file.Events, e => e.Name == "exception");
    }

    [Fact]
    public async Task ExecuteAsync_LogsEachFileWithinAScopeNamingIt()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var provider = fakes.CreateServices().BuildServiceProvider();
        var logger = new ScopeRecordingLogger();
        var sweep = ExtractorSweepTestProcess.Definition().CreateJob(provider, new SingleLoggerFactory(logger));

        await sweep.ExecuteAsync(CancellationToken.None);

        var handled = Assert.Single(logger.Entries, e => e.Text.Contains("Handled source file"));
        var scope = Assert.Single(handled.Scopes.OfType<IReadOnlyDictionary<string, object>>());
        Assert.Equal("order-1.json", scope["FileName"]);
        Assert.Equal(ExtractorSweepTestProcess.SourceKey, scope["SourcePort"]);
    }

    /// <summary>Runs <paramref name="run"/> under a test-owned job span and returns that span, the
    /// file spans linked to it, and every span in those files' traces. Spans from tests running in
    /// parallel are in other traces and are ignored.</summary>
    private static async Task<(Activity Job, List<Activity> Files, List<Activity> Spans)> CaptureFileTracesAsync(
        Func<CancellationToken, Task> run)
    {
        using var jobSource = new ActivitySource($"extractor-tracing-test-{Guid.NewGuid()}");
        var stopped = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source == jobSource ||
                source.Name is HostingActivitySource or PipelineActivitySource,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (stopped) stopped.Add(activity); }
        };
        ActivitySource.AddActivityListener(listener);

        Activity job;
        using (job = jobSource.StartActivity("job")!)
            await run(CancellationToken.None);

        lock (stopped)
        {
            var files = stopped.Where(a => a.Source.Name == HostingActivitySource && a.DisplayName == FileSpanName &&
                a.Links.Any(l => l.Context.SpanId == job.SpanId)).ToList();
            var traces = files.Select(f => f.TraceId).ToHashSet();
            return (job, files, stopped.Where(a => traces.Contains(a.TraceId)).ToList());
        }
    }

    private static ExtractorJob<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>
        CreateSweep(ServiceCollection services)
    {
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        return provider.GetRequiredService<TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>>()
            .CreateJob(provider, NullLoggerFactory.Instance);
    }

    /// <summary>Records the trace each read and delete ran in.</summary>
    private sealed class TraceRecordingFileAdapter(IFileAdapter inner) : IFileAdapter
    {
        private readonly List<(string Operation, string FileName, ActivityTraceId? TraceId)> _calls = [];

        public ActivityTraceId? TraceOf(string operation, string fileName) =>
            Assert.Single(_calls, c => c.Operation == operation && c.FileName == fileName).TraceId;

        public Task<List<FileEntry>> ListAsync(CancellationToken ct = default) => inner.ListAsync(ct);

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default)
        {
            Record("get", fileName);
            return inner.GetContentAsync(fileName, ct);
        }

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null, CancellationToken ct = default) =>
            inner.WriteAsync(fileName, content, basePathOverride, ct);

        public Task DeleteAsync(string fileName, CancellationToken ct = default)
        {
            Record("delete", fileName);
            return inner.DeleteAsync(fileName, ct);
        }

        private void Record(string operation, string fileName) =>
            _calls.Add((operation, fileName, Activity.Current?.TraceId));
    }

    /// <summary>One logger for every category, sharing one scope stack the way a real logger
    /// factory shares its scope provider.</summary>
    private sealed class ScopeRecordingLogger : ILogger
    {
        private readonly List<object> _scopes = [];

        public List<(string Text, object[] Scopes)> Entries { get; } = [];

        public IDisposable BeginScope<TState>(TState state) where TState : notnull
        {
            _scopes.Add(state);
            return new Scope(() => _scopes.Remove(state));
        }

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Entries.Add((formatter(state, exception), [.. _scopes]));

        private sealed class Scope(Action dispose) : IDisposable
        {
            public void Dispose() => dispose();
        }
    }

    private sealed class SingleLoggerFactory(ILogger logger) : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();

        public ILogger CreateLogger(string categoryName) => logger;

        public void Dispose() { }
    }
}
