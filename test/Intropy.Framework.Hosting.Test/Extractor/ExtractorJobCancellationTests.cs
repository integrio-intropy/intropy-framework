using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Testing.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// The sweep's cancellation contract: business duplicates (<c>StepResult.Cancelled</c>)
/// result from idempotency checks, never from a host abort; cancellation is honored before
/// listing and before each new item; in-flight, uninterruptible I/O is always awaited; a
/// pipeline that returns Aborted never has its source deleted; aborted/unstarted files are
/// not counted, recorded failures are never erased, and an unexpected
/// <see cref="OperationCanceledException"/> without a host cancellation cannot produce a
/// "successful" sweep.
/// </summary>
public class ExtractorJobCancellationTests
{
    private const string ValidInput =
        """{"Id":"input-42","Version":"2026-01-15T09:30:00+00:00","Valid":true}""";

    [Fact]
    public async Task ExecuteAsync_WhenCancelledBeforeListing_ProcessesNothing()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(cts.Token);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, summary.Failed);
        Assert.Empty(fakes.Topic.Events);
        Assert.Single(fakes.Files.Files); // retained
    }

    [Fact]
    public async Task ExecuteAsync_WhenCancelledBetweenFiles_StopsStartingNewWork_AndPreservesCounts()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        using var cts = new CancellationTokenSource();
        var adapter = new ExtractorSweepTestProcess.CancelAfterDeleteFileAdapter(fakes.Files, cts);
        var sweep = CreateSweep(fakes.CreateServices(adapterOverride: adapter));

        var summary = await sweep.ExecuteAsync(cts.Token);

        // The first file completed after cancellation arrived mid-run (its delete was
        // awaited, never abandoned); the second was never started and is not counted.
        Assert.Equal(1, summary.Processed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, summary.Failed);
        Assert.Single(fakes.Topic.Events);
        Assert.Single(fakes.Files.Files); // order-2.json retained
        Assert.DoesNotContain(fakes.Files.Files.Keys, k => k == "order-1.json");
    }

    [Fact]
    public async Task ExecuteAsync_WhenCancelledAfterFailedFiles_PreservesRecordedFailures()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("broken.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        fakes.Topic.SendException = new InvalidOperationException("Broker unreachable");
        using var cts = new CancellationTokenSource();
        // After the first failure, order-2 reads fine but its topic send is cancelling:
        // the pre-send abort of the remaining file must not erase the recorded failure.
        var adapter = new ExtractorSweepTestProcess.CancelAfterReadFileAdapter(fakes.Files, cts, "order-2.json");
        var sweep = CreateSweep(fakes.CreateServices(adapterOverride: adapter));

        var summary = await sweep.ExecuteAsync(cts.Token);

        Assert.Equal(1, summary.Failed); // cancellation did not erase the failure
        Assert.Equal(0, summary.Skipped); // aborted/unstarted files are not duplicate cancellations
        Assert.Equal(0, summary.Processed);
        Assert.Empty(fakes.Topic.Events);
        Assert.Equal(2, fakes.Files.Files.Count); // both files retained
    }

    [Fact]
    public async Task ExecuteAsync_WhenPipelineAbortsUnderHostCancellation_RetainsFileWithoutCounting()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        using var cts = new CancellationTokenSource();
        var services = fakes.CreateEdgeServices();
        services.AddSingleton(new ExtractorSweepTestProcess.CancelAbortingValidator(cts, requestCancel: true));
        services.AddExtractor(ExtractorSweepTestProcess.AbortingDefinition());
        var sweep = CreateSweep(services);

        var summary = await sweep.ExecuteAsync(cts.Token);

        // Aborted because the host asked to cancel: the file is retained, counted as
        // neither failure nor duplicate cancellation, and no further files are started.
        Assert.Equal(0, summary.Processed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, summary.Failed);
        Assert.Empty(fakes.Topic.Events);
        Assert.Equal(2, fakes.Files.Files.Count);
    }

    [Fact]
    public async Task ExecuteAsync_WhenPipelineAbortsWithoutHostCancellation_RetainsFileAndCountsFailed()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        using var cts = new CancellationTokenSource(); // never cancelled
        var services = fakes.CreateEdgeServices();
        services.AddSingleton(new ExtractorSweepTestProcess.CancelAbortingValidator(cts, requestCancel: false));
        services.AddExtractor(ExtractorSweepTestProcess.AbortingDefinition());
        var sweep = CreateSweep(services);

        var summary = await sweep.ExecuteAsync(cts.Token);

        // An abort without a requested host cancellation is not a successful duplicate.
        Assert.Equal(1, summary.Failed);
        Assert.Equal(0, summary.Skipped);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
    }

    [Fact]
    public async Task ExecuteAsync_WhenHostCancellationArrivesDuringUninterruptibleRead_CompletesTheReadThenRetains()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        using var cts = new CancellationTokenSource();
        var adapter = new ExtractorSweepTestProcess.CancelDuringReadFileAdapter(fakes.Files, cts);
        var sweep = CreateSweep(fakes.CreateServices(adapterOverride: adapter));

        var summary = await sweep.ExecuteAsync(cts.Token);

        // The read cannot be cancelled and was awaited to completion; the pipeline then ran
        // with a cancelled token and aborted. The file is retained and uncounted; no
        // in-flight work was abandoned in the background.
        Assert.Equal(0, summary.Processed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, summary.Failed);
        Assert.Empty(fakes.Topic.Events);
        Assert.Single(fakes.Files.Files);
    }

    [Fact]
    public async Task ExecuteAsync_WhenReadThrowsOceWithoutHostCancellation_CountsFailedAndContinues()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("instable.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        using var cts = new CancellationTokenSource();
        var adapter = new ExtractorSweepTestProcess.OceReadFileAdapter(fakes.Files, cts, cancelFirst: false, "instable.json");
        var sweep = CreateSweep(fakes.CreateServices(adapterOverride: adapter));

        var summary = await sweep.ExecuteAsync(cts.Token);

        // An OperationCanceledException without a requested host cancellation is a real
        // failure of the file — it must not surface as a duplicate cancellation nor be
        // skipped silently; the sweep continues with the next file.
        Assert.Equal(1, summary.Failed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(1, summary.Processed);
        Assert.True(fakes.Files.Files.ContainsKey("instable.json"));
        Assert.DoesNotContain(fakes.Files.Files.Keys, k => k == "order-2.json");
    }

    [Fact]
    public async Task ExecuteAsync_WhenReadThrowsOceWithHostCancellation_StopsAndDoesNotFailTheFile()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        using var cts = new CancellationTokenSource();
        var adapter = new ExtractorSweepTestProcess.OceReadFileAdapter(fakes.Files, cts, cancelFirst: true, "order-1.json");
        var sweep = CreateSweep(fakes.CreateServices(adapterOverride: adapter));

        var summary = await sweep.ExecuteAsync(cts.Token);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, summary.Failed);
        Assert.Single(fakes.Files.Files); // retained for the next run
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task ExecuteAsync_CancelledDuplicateStillConsumes_WhileAbortedNeverDeletes()
    {
        // Pins the distinction: StepResult.Cancelled (idempotency) deletes the file; a
        // pipeline Aborted under cancellation does not.
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("duplicate.json", ValidInput);
        fakes.Idempotency.NextStatus = new Intropy.Contracts.IdempotencyService.StatusResponse(
            Intropy.Contracts.IdempotencyService.Action.Ignore,
            Intropy.Contracts.IdempotencyService.Reason.SameData);
        var sweep = CreateSweep(fakes.CreateServices());

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Skipped); // business duplicate — deleted, counted
        Assert.Empty(fakes.Files.Files);
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task ExecuteAsync_WhenCancelledDuringCompositionValidation_NeverStartsListing()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        using var cts = new CancellationTokenSource();
        var countingAdapter = new ExtractorSweepTestProcess.CountingListFileAdapter(fakes.Files);
        var services = fakes.CreateEdgeServices(adapterOverride: countingAdapter);
        // The dependency construction inside the composition-validation scope cancels the
        // host token; the sweep must re-check before starting source I/O.
        services.AddScoped<ExtractorTestProcess.ScopedDependency>(_ =>
        {
            cts.Cancel();
            return new ExtractorTestProcess.ScopedDependency();
        });
        services.AddExtractor(ExtractorSweepTestProcess.Definition());
        var sweep = CreateSweep(services);

        var summary = await sweep.ExecuteAsync(cts.Token);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(0, summary.Failed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, countingAdapter.ListCalls); // cancellation checked before source I/O
        Assert.Single(fakes.Files.Files);
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task ExecuteAsync_WhenScopeCleanupThrowsOce_PreservesAccumulatedFailures()
    {
        // Every processing scope (and the preflight scope) disposes a dependency whose second
        // and later instances throw OperationCanceledException on DisposeAsync. The preflight
        // scope cleans up quietly; both file scopes fail cleanup after their outcomes were
        // recorded. The escaping cleanup OCE must not reach the runner — it would map the
        // whole run to a successful exit and discard the recorded failure.
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddUnreadableFile("broken.json");
        fakes.Files.AddFile("order-2.json", ValidInput);
        var created = 0;
        var definition = ExtractorSweepTestProcess.Definition()
            with { Deserializer = typeof(ExtractorSweepTestProcess.CleanupOceDeserializer) };
        var services = fakes.CreateEdgeServices();
        services.AddExtractor(definition);
        // The preflight scope (instance 0) cleans up quietly; every later processing scope's
        // cleanup throws OperationCanceledException.
        services.AddScoped<ExtractorSweepTestProcess.ScopeCleanupOceDependency>(_
            => new ExtractorSweepTestProcess.ScopeCleanupOceDependency(throwOnDispose: created++ > 0));
        var sweep = CreateSweep(services);

        var summary = await sweep.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Equal(1, summary.Failed); // the earlier failure stands
        Assert.Equal(0, summary.Skipped);
        Assert.Single(fakes.Topic.Events);
    }

    [Fact]
    public async Task ExecuteAsync_WhenPreflightThrowsUnexpectedCancellation_PropagatesBeforeListing()
    {
        // The job does not translate a preflight cancellation: it reaches the runner, which
        // treats a cancellation the host never requested as a job failure.
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var services = fakes.CreateEdgeServices();
        // During preflight, dependency construction cancels — the host never asked for it.
        services.RemoveAll<ExtractorTestProcess.ScopedDependency>();
        services.AddScoped<ExtractorTestProcess.ScopedDependency>(_ =>
            throw new OperationCanceledException("A dependency construction was cancelled"));
        services.AddExtractor(ExtractorSweepTestProcess.Definition());
        var sweep = CreateSweep(services);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => sweep.ExecuteAsync(CancellationToken.None));

        Assert.Single(fakes.Files.Files);
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task ExecuteAsync_WhenCleanupOnlyCancellation_FailsTheJobWithoutLosingOutcomes()
    {
        // The one file is processed cleanly, but its scope cleanup observes an unexpected
        // cancellation: the run must not report success — it escalates to a job failure with
        // the recorded outcomes intact (the file was still published and consumed).
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var created = 0;
        var definition = ExtractorSweepTestProcess.Definition()
            with { Deserializer = typeof(ExtractorSweepTestProcess.CleanupOceDeserializer) };
        var services = fakes.CreateEdgeServices();
        services.AddExtractor(definition);
        services.AddScoped<ExtractorSweepTestProcess.ScopeCleanupOceDependency>(_
            => new ExtractorSweepTestProcess.ScopeCleanupOceDependency(throwOnDispose: created++ > 0));
        var sweep = CreateSweep(services);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => sweep.ExecuteAsync(CancellationToken.None));

        Assert.Contains("unexpected cancellation", error.Message);
        Assert.Contains("1 processed", error.Message); // recorded outcomes are part of the error
        Assert.Single(fakes.Topic.Events);              // the publication still happened
        Assert.Empty(fakes.Files.Files);                // the file was still consumed
    }

    [Fact]
    public async Task ExecuteAsync_WhenCancelledDuringAdapterConstruction_NeverStartsListing()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        using var cts = new CancellationTokenSource();
        var countingAdapter = new ExtractorSweepTestProcess.CountingListFileAdapter(fakes.Files);
        var services = fakes.CreateEdgeServices();
        // The singleton adapter's construction (during the preflight) cancels the host token
        // without throwing — the sweep must re-check the token before starting source I/O.
        services.AddKeyedSingleton<IFileAdapter>(ExtractorSweepTestProcess.SourceKey, (_, _) =>
        {
            cts.Cancel();
            return countingAdapter;
        });
        services.AddExtractor(ExtractorSweepTestProcess.Definition());
        var sweep = CreateSweep(services);

        var summary = await sweep.ExecuteAsync(cts.Token);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(0, summary.Failed);
        Assert.Equal(0, summary.Skipped);
        Assert.Equal(0, countingAdapter.ListCalls); // cancellation checked before listing
        Assert.Single(fakes.Files.Files);
        Assert.Empty(fakes.Topic.Events);
    }

    private static ExtractorJob<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>
        CreateSweep(IServiceCollection services)
    {
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        return provider.GetRequiredService<TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>>().CreateJob(provider, NullLoggerFactory.Instance);
    }
}
