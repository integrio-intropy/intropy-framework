using System.Text;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.Sweep;
using Intropy.Framework.Testing.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using IdempotencyAction = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// The file sweep's consumption contract, exercised against the real production composition
/// with only the four externals faked: published, duplicate-cancelled, and incident-routed
/// files are consumed (deleted); technical failures, unreadable files, empty content, and
/// delete failures keep the file and are counted — a mixed batch produces exact counts, and
/// a listing failure fails the job instead of reporting an empty successful sweep.
/// </summary>
public class ExtractorJobTests
{
    private const string ValidInput =
        """{"Id":"input-42","Version":"2026-01-15T09:30:00+00:00","Valid":true}""";
    private const string InvalidInput =
        """{"Id":"input-42","Version":"2026-01-15T09:30:00+00:00","Valid":false}""";

    [Fact]
    public async Task ExecuteAsync_WhenSourceIsEmpty_ReturnsEmptySummary()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, summary.Failed);
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task ExecuteAsync_WithValidFile_PublishesCommitsDeletesAndCountsProcessed()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, summary.Failed);
        var published = Assert.Single(fakes.Topic.Events);
        Assert.Equal("event-input-42", published.Subject);
        Assert.Empty(fakes.Files.Files);
        var commit = Assert.Single(fakes.Idempotency.Committed);
        Assert.Equal("input-42", commit.Id);
    }

    [Fact]
    public async Task ExecuteAsync_WithArchiveCompletion_ArchivesThePublishedFile()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var definition = ExtractorSweepTestProcess.Definition() with { Completion = SweepCompletion.Archive("archive") };
        var sweep = CreateSweep(fakes.CreateServices(definition: definition));

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Single(fakes.Topic.Events);
        Assert.False(fakes.Files.Files.ContainsKey("order-1.json"));
        Assert.Equal(ValidInput, fakes.Files.GetString("archive", "order-1.json"));
    }

    [Fact]
    public async Task ExecuteAsync_WithDuplicate_CancelsDeletesWithoutPublishingOrCommitting()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Idempotency.NextStatus = new Intropy.Contracts.IdempotencyService.StatusResponse(
            IdempotencyAction.Ignore, Intropy.Contracts.IdempotencyService.Reason.SameData);
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(1, summary.Skipped);
        Assert.Equal(0, summary.Failed);
        Assert.Empty(fakes.Topic.Events);
        Assert.Empty(fakes.Idempotency.Committed);
        Assert.Empty(fakes.Files.Files);
    }

    [Fact]
    public async Task ExecuteAsync_WithValidationFailure_RoutesIncidentAndConsumesFile()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", InvalidInput);
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        // A successfully routed business incident consumes the file, counts as processed,
        // publishes nothing, and commits no idempotency record.
        Assert.Equal(1, summary.Processed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, summary.Failed);
        Assert.Empty(fakes.Topic.Events);
        Assert.Empty(fakes.Idempotency.Committed);
        Assert.Empty(fakes.Files.Files);
        var incident = Assert.Single(fakes.Incidents.Incidents);
        Assert.Equal("Input rejected", incident.Data.Description);
    }

    [Fact]
    public async Task ExecuteAsync_WhenIncidentServiceFails_RetainsFileAndCountsFailed()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", InvalidInput);
        fakes.Incidents.TriggerException = new BusinessIncidentServiceException("unavailable", new HttpRequestException());
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(1, summary.Failed);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task ExecuteAsync_WhenBrokerIsDown_RetainsFileAndCountsFailed()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Topic.SendException = new InvalidOperationException("Broker unreachable");
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Failed);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task ExecuteAsync_WithUnreadableFile_FailsItAndProcessesLaterValidFile()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddUnreadableFile("corrupt.json");
        fakes.Files.AddFile("order-2.json", ValidInput);
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Equal(1, summary.Failed);
        Assert.Single(fakes.Topic.Events);
        Assert.False(fakes.Files.Files.ContainsKey("order-2.json"), "a bad file never blocks the batch");
        Assert.Contains("corrupt.json", (await fakes.Files.ListAsync()).Select(f => f.Name));
    }

    [Fact]
    public async Task ExecuteAsync_WithEmptyContent_RetainsFileAndCountsFailed()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var emptyContentAdapter = new ExtractorSweepTestProcess.EmptyContentFileAdapter(fakes.Files, "order-1.json");
        var sweep = CreateSweep(fakes.CreateServices(adapterOverride: emptyContentAdapter));

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(1, summary.Failed);
        Assert.Empty(fakes.Topic.Events);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"), "a listed file with no content is retained");
    }

    [Fact]
    public async Task ExecuteAsync_WithEmptySourceFile_FailsTheRunOnAnyAdapter()
    {
        // An empty file reads as string.Empty on every adapter — and empty content is a stuck
        // file: it fails the run (the job returns a summary whose Failed count maps to exit 1)
        // instead of silently skipping the file or routing it as an incident.
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", string.Empty);
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(1, summary.Failed);
        Assert.Empty(fakes.Incidents.Incidents);
        Assert.Empty(fakes.Topic.Events);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"), "an empty source file is retained for the next run");
    }

    [Fact]
    public async Task ExecuteAsync_WhenDeleteFailsAfterPublication_RetainsFileAndCountsFailed()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.SetDeleteException("order-1.json", new InvalidOperationException("Source refuses deletes"));
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(1, summary.Failed);
        Assert.Single(fakes.Topic.Events); // published, but the file stays
        Assert.Single(fakes.Idempotency.Committed);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
    }

    [Fact]
    public async Task ExecuteAsync_WhenDeleteFailsAfterDuplicate_RetainsFileAndCountsFailed()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Idempotency.NextStatus = new Intropy.Contracts.IdempotencyService.StatusResponse(
            IdempotencyAction.Ignore, Intropy.Contracts.IdempotencyService.Reason.SameData);
        fakes.Files.SetDeleteException("order-1.json", new InvalidOperationException("Source refuses deletes"));
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(0, summary.Skipped);
        Assert.Equal(1, summary.Failed);
        Assert.Empty(fakes.Topic.Events);
        Assert.Empty(fakes.Idempotency.Committed);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
    }

    [Fact]
    public async Task ExecuteAsync_RedeliveredDuplicateAfterFailedDelete_DeletesWithoutRepublishing()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.SetDeleteException("order-1.json", new InvalidOperationException("Source refuses deletes"));

        var first = await CreateSweep(fakes.CreateServices()).ExecuteAsync(CancellationToken.None);

        // Second run: the idempotency service now has the commit from run one, so the
        // redelivery checks out as a duplicate; the delete succeeds this time.
        fakes.Idempotency.NextStatus = new Intropy.Contracts.IdempotencyService.StatusResponse(
            IdempotencyAction.Ignore, Intropy.Contracts.IdempotencyService.Reason.SameData);
        fakes.Files.SetDeleteException("order-1.json", null);

        var second = await CreateSweep(fakes.CreateServices()).ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, first.Failed);
        Assert.Equal(0, second.Failed);
        Assert.Equal(1, second.Skipped);
        Assert.Single(fakes.Topic.Events); // no double-publish
        Assert.Single(fakes.Idempotency.Committed); // no second commit
        Assert.Empty(fakes.Files.Files);
    }

    [Fact]
    public async Task ExecuteAsync_WithMixedBatch_ProducesExactCounts()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-a.json", ValidInput);        // publishes → Processed
        fakes.Files.AddUnreadableFile("corrupt.json");          // read failure → Failed
        fakes.Files.AddFile("order-b.json", ValidInput);        // duplicate → Cancelled
        fakes.Idempotency.QueueStatus(new Intropy.Contracts.IdempotencyService.StatusResponse(
            IdempotencyAction.Ignore, Intropy.Contracts.IdempotencyService.Reason.SameData));
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Equal(1, summary.Skipped);
        Assert.Equal(1, summary.Failed);
        Assert.Single(fakes.Topic.Events);
        Assert.Single(fakes.Idempotency.Committed);
        var listed = await fakes.Files.ListAsync();
        var remaining = Assert.Single(listed.Select(f => f.Name));
        Assert.Equal("corrupt.json", remaining);
    }

    [Fact]
    public async Task ExecuteAsync_WhenListingFails_FailsTheJob()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.ReadException = new InvalidOperationException("Source unreachable");
        var sweep = CreateSweep(fakes.CreateServices());

        await Assert.ThrowsAsync<InvalidOperationException>(() => sweep.ExecuteAsync(CancellationToken.None));

        Assert.Empty(fakes.Topic.Events);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
    }

    [Fact]
    public async Task ExecuteAsync_WhenContextFactoryReturnsNull_RetainsFileAndCountsFailed()
    {
        // Item setup runs inside the outcome boundary: a factory defect fails exactly its
        // file and the sweep continues with the rest of the batch.
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        var nil = default(ExtractorTestProcess.TestContext);
        var sweep = CreateSweep(fakes.CreateServices(definition: ExtractorSweepTestProcess.Definition()
            with { ContextFactory = (_, _) => nil! }));

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(2, summary.Failed);
        Assert.Empty(fakes.Topic.Events);
        Assert.Equal(2, fakes.Files.Files.Count); // both files retained
    }

    [Fact]
    public async Task ExecuteAsync_WhenContextFactoryThrows_CountsOneFailureAndContinues()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        var sweep = CreateSweep(fakes.CreateServices(definition: ExtractorSweepTestProcess.Definition()
            with { ContextFactory = ExtractorTestProcess.ByFileName(fileName => fileName == "order-1.json"
                ? throw new IOException("Factory cannot prepare state")
                : new ExtractorTestProcess.TestContext()) }));

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        // The broken factory cost exactly one failed file; the batch was not aborted.
        Assert.Equal(1, summary.Processed);
        Assert.Equal(1, summary.Failed);
        Assert.Single(fakes.Topic.Events);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
    }

    [Fact]
    public async Task ExecuteAsync_WhenContextFactoryThrowsUnexpectedCancellation_CountsFailedAndContinues()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        var sweep = CreateSweep(fakes.CreateServices(definition: ExtractorSweepTestProcess.Definition()
            with { ContextFactory = ExtractorTestProcess.ByFileName(fileName => fileName == "order-1.json"
                ? throw new OperationCanceledException("A dependency the factory awaits was cancelled")
                : new ExtractorTestProcess.TestContext()) }));

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        // A cancellation the host never requested must not become a failed-abort or a
        // successful run: one failed file, and the batch continues.
        Assert.Equal(1, summary.Processed);
        Assert.Equal(1, summary.Failed);
        Assert.Equal(0, summary.Skipped);
        Assert.Single(fakes.Topic.Events);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
    }

    [Fact]
    public async Task ExecuteAsync_WhenContextFactoryThrowsHostCancellationAfterAFailure_PreservesCounts()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("broken.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        fakes.Topic.SendException = new InvalidOperationException("Broker unreachable");
        using var cts = new CancellationTokenSource();
        var sweep = CreateSweep(fakes.CreateServices(definition: ExtractorSweepTestProcess.Definition()
            with { ContextFactory = ExtractorTestProcess.ByFileName(fileName =>
            {
                if (fileName == "order-2.json")
                {
                    cts.Cancel();
                    throw new OperationCanceledException("Dependent work was cancelled");
                }
                return new ExtractorTestProcess.TestContext();
            }) }));

        var summary = await sweep.ExecuteAsync(cts.Token);

        // The host-requested interruption stops the sweep but must not erase the failure
        // recorded for the first file — the runner maps this summary to exit 1.
        Assert.Equal(1, summary.Failed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, summary.Processed);
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task ExecuteAsync_WhenListingThrowsUnexpectedCancellation_FailsTheJob()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        using var cts = new CancellationTokenSource();
        var adapter = new ExtractorSweepTestProcess.OceListFileAdapter(fakes.Files, cts, cancelFirst: false);
        var sweep = CreateSweep(fakes.CreateServices(adapterOverride: adapter));

        // The sweep normalizes a listing cancellation the host never requested into a job failure.
        await Assert.ThrowsAsync<InvalidOperationException>(() => sweep.ExecuteAsync(CancellationToken.None));

        Assert.Empty(fakes.Topic.Events);
        Assert.Single(fakes.Files.Files); // source untouched
    }

    [Fact]
    public async Task ExecuteAsync_LogsHandledFilesWithoutClaimingPublication()
    {
        // Success covers publication OR a routed incident; the sweep must not log every
        // success as "Published". This test pins the incident-routed file's log line.
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", InvalidInput);
        var provider = fakes.CreateServices().BuildServiceProvider();
        var logger = new RecordingLogger();
        var sweepLoggerFactory = new SingleLoggerFactory(logger);
        var sweep = ExtractorSweepTestProcess.Definition().CreateJob(provider, sweepLoggerFactory);

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Single(fakes.Incidents.Incidents);
        Assert.Empty(fakes.Topic.Events);
        Assert.All(logger.Messages, m => Assert.DoesNotContain("Published", m.Text));
        var handled = Assert.Single(logger.Messages, m => m.Text.Contains("Handled source file"));
        Assert.Contains("order-1.json", handled.Text);
    }

    private static ExtractorJob<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>
        CreateSweep(ServiceCollection services)
    {
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        return provider.GetRequiredService<TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>>().CreateJob(provider, NullLoggerFactory.Instance);
    }

    private sealed class RecordingLogger : ILogger
    {
        public List<(LogLevel Level, string Text)> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Messages.Add((logLevel, formatter(state, exception)));
    }

    private sealed class SingleLoggerFactory(ILogger logger) : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();

        public ILogger CreateLogger(string categoryName) => logger;

        public void Dispose() { }
    }
}
