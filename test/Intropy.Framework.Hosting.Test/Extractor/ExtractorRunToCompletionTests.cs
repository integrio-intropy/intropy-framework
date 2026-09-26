using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.Sweep;
using Intropy.Framework.Hosting.RunToCompletion;
using Intropy.Framework.Testing.Adapters;
using Intropy.Framework.Testing.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// Registration and runner integration for the sweep-driven extractor: the complete graph
/// resolves without external calls, missing dependencies and duplicate components fail
/// clearly before any file is processed, the singleton job never captures a scoped
/// pipeline, the runner receives the sweep outcome unchanged (exit 0 for clean runs and
/// duplicates, exit 1 for file failures), and direct sweep execution needs no sidecar.
/// </summary>
public class ExtractorRunToCompletionTests
{
    private const string ValidInput =
        """{"Id":"input-42","Version":"2026-01-15T09:30:00+00:00","Valid":true}""";

    [Fact]
    public async Task Registration_ComposesEverythingAndGraphResolvesWithoutExternalCalls()
    {
        var (provider, _) = BuildRegistration();

        // The explicit composition check resolves the full graph in a scope, without a sidecar.
        await provider.ValidateExtractorCompositionAsync<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>();

        var job = provider.GetRequiredService<IRunToCompletionJob>();
        Assert.IsType<ExtractorJob<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>>(job);
        Assert.Equal("orders-extractor", provider.GetRequiredService<RunToCompletionOptions>().JobName);
        Assert.Empty(provider.GetRequiredService<DaprClient>().ReceivedCalls()); // no external calls
    }

    [Fact]
    public void Registration_DefaultJobNameDerivesFromComponentName_AndOptionsAreConfigurable()
    {
        var (provider, _) = BuildRegistration(options => options.SidecarTimeout = TimeSpan.FromSeconds(42));

        var options = provider.GetRequiredService<RunToCompletionOptions>();
        Assert.Equal("orders-extractor", options.JobName); // the component name
        Assert.Equal(TimeSpan.FromSeconds(42), options.SidecarTimeout);   // existing runner options, no second options model
    }

    [Fact]
    public async Task MissingSourcePort_FailsBeforeAnyFileIsProcessed()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var services = fakes.CreateEdgeServices();
        services.AddExtractor(ExtractorSweepTestProcess.Definition());
        services.RemoveAll<SourcePort>();
        await using var provider = services.BuildServiceProvider();

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => provider.GetRequiredService<IRunToCompletionJob>().ExecuteAsync(CancellationToken.None));

        Assert.Contains("AddSourcePort", error.Message);
        Assert.Single(fakes.Files.Files);
    }

    [Fact]
    public async Task MissingComponentIdentity_FailsBeforeAnyFileIsProcessed()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var services = fakes.CreateEdgeServices();
        services.AddExtractor(ExtractorSweepTestProcess.Definition(), options => options.JobName = "orders");
        services.RemoveAll<FrameworkOptions>();
        await using var provider = services.BuildServiceProvider();

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => provider.GetRequiredService<IRunToCompletionJob>().ExecuteAsync(CancellationToken.None));

        Assert.Contains(nameof(FrameworkOptions), error.Message);
        Assert.Single(fakes.Files.Files);
    }

    [Fact]
    public void Registration_SecondComponentIsRejected()
    {
        var definition = ExtractorSweepTestProcess.Definition();
        var services = RegisterFull(new ServiceCollection(), definition);

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.AddExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
                ExtractorTestProcess.TestContext>((builder, _) => builder, definition.ContextFactory));

        // The one-component-per-provider guard rejects a second extractor rather than silently
        // registering it.
        Assert.Contains("Only one extractor component", error.Message);
    }

    [Fact]
    public async Task Runner_CompletesCleanSweep_WithExitZero()
    {
        var (provider, fakes) = BuildRegistration();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var runner = provider.GetRequiredService<RunToCompletionRunner>();

        var exitCode = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.Success, exitCode);
        await provider.GetRequiredService<DaprClient>().Received(1).WaitForSidecarAsync(Arg.Any<CancellationToken>());
        await provider.GetRequiredService<DaprClient>().Received(1).ShutdownSidecarAsync(Arg.Any<CancellationToken>());
        Assert.Single(fakes.Topic.Events);
        Assert.Empty(fakes.Files.Files);
    }

    [Fact]
    public async Task Runner_DuplicateOnlySweep_ResultsInExitZero()
    {
        var (provider, fakes) = BuildRegistration();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Idempotency.NextStatus = new Intropy.Contracts.IdempotencyService.StatusResponse(
            Intropy.Contracts.IdempotencyService.Action.Ignore, Intropy.Contracts.IdempotencyService.Reason.SameData);
        var runner = provider.GetRequiredService<RunToCompletionRunner>();

        var exitCode = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.Success, exitCode);
        Assert.Empty(fakes.Topic.Events);
        Assert.Empty(fakes.Files.Files); // deleted as a duplicate
    }

    [Fact]
    public async Task Runner_EmptySourceFile_ResultsInExitOne()
    {
        var (provider, fakes) = BuildRegistration();
        fakes.Files.AddFile("order-1.json", string.Empty);
        var runner = provider.GetRequiredService<RunToCompletionRunner>();

        var exitCode = await runner.RunAsync();

        // An empty source file reads as string.Empty on every adapter and is a stuck file:
        // the run fails with exit 1 and the file stays for the next run.
        Assert.Equal(RunToCompletionExitCodes.JobFailure, exitCode);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task Runner_FileFailures_ResultInExitOne()
    {
        var (provider, fakes) = BuildRegistration();
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Topic.SendException = new InvalidOperationException("Broker unreachable");
        var runner = provider.GetRequiredService<RunToCompletionRunner>();

        var exitCode = await runner.RunAsync();

        Assert.Equal(RunToCompletionExitCodes.JobFailure, exitCode);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
    }

    [Fact]
    public async Task Runner_CancelledAfterRecordedFailures_ExitsOne()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("broken.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        fakes.Topic.SendException = new InvalidOperationException("Broker unreachable");
        using var cts = new CancellationTokenSource();
        var services = fakes.CreateEdgeServices();
        services.AddKeyedSingleton<IFileAdapter>(ExtractorSweepTestProcess.SourceKey,
            new ExtractorSweepTestProcess.CancelAfterReadFileAdapter(fakes.Files, cts, "order-2.json"));
        services.AddExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
            ExtractorTestProcess.TestContext>(ExtractorSweepTestProcess.Definition());
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var _ = provider;
        var runner = provider.GetRequiredService<RunToCompletionRunner>();

        var exitCode = await runner.RunAsync(cts.Token);

        // Cancellation must not erase previously recorded failures.
        Assert.Equal(RunToCompletionExitCodes.JobFailure, exitCode);
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task Runner_UnexpectedCancellationWithoutHostRequest_CountsAsFailure_ExitsOne()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        using var cts = new CancellationTokenSource();
        var services = fakes.CreateEdgeServices();
        services.AddKeyedSingleton<IFileAdapter>(ExtractorSweepTestProcess.SourceKey,
            new ExtractorSweepTestProcess.OceReadFileAdapter(fakes.Files, cts, cancelFirst: false, "order-1.json"));
        services.AddExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
            ExtractorTestProcess.TestContext>(ExtractorSweepTestProcess.Definition());
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var _ = provider;
        var runner = provider.GetRequiredService<RunToCompletionRunner>();

        var exitCode = await runner.RunAsync();

        // An OperationCanceledException with no host cancellation must not become a
        // successful run: the file failed.
        Assert.Equal(RunToCompletionExitCodes.JobFailure, exitCode);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
    }

    [Fact]
    public async Task Runner_UnexpectedCancellationDuringListing_CountsAsFailure_ExitsOne()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        using var cts = new CancellationTokenSource();
        var services = fakes.CreateEdgeServices();
        services.AddKeyedSingleton<IFileAdapter>(ExtractorSweepTestProcess.SourceKey,
            new ExtractorSweepTestProcess.OceListFileAdapter(fakes.Files, cts, cancelFirst: false));
        services.AddExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
            ExtractorTestProcess.TestContext>(ExtractorSweepTestProcess.Definition());
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var _ = provider;
        var runner = provider.GetRequiredService<RunToCompletionRunner>();

        var exitCode = await runner.RunAsync();

        // An OperationCanceledException from the listing the host never requested fails
        // the job instead of reporting an empty successful sweep.
        Assert.Equal(RunToCompletionExitCodes.JobFailure, exitCode);
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
    }

    [Fact]
    public async Task Runner_ScopeCleanupCancellationAfterFailures_StillExitsOne()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("broken.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        fakes.Topic.SendException = new InvalidOperationException("Broker unreachable");
        var created = 0;
        var definition = ExtractorSweepTestProcess.Definition()
            with { Deserializer = typeof(ExtractorSweepTestProcess.CleanupOceDeserializer) };
        var services = fakes.CreateEdgeServices();
        services.AddExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
            ExtractorTestProcess.TestContext>(definition);
        services.AddScoped<ExtractorSweepTestProcess.ScopeCleanupOceDependency>(_
            => new ExtractorSweepTestProcess.ScopeCleanupOceDependency(throwOnDispose: created++ > 0));
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var _ = provider;
        var runner = provider.GetRequiredService<RunToCompletionRunner>();

        var exitCode = await runner.RunAsync();

        // An escaping cleanup OCE never hides the recorded failure: it maps to exit 1.
        Assert.Equal(RunToCompletionExitCodes.JobFailure, exitCode);
    }

    [Fact]
    public async Task Runner_CleanupOnlyCancellation_FailsTheJobWithOutcomesIntact()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var created = 0;
        var definition = ExtractorSweepTestProcess.Definition()
            with { Deserializer = typeof(ExtractorSweepTestProcess.CleanupOceDeserializer) };
        var services = fakes.CreateEdgeServices();
        services.AddExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
            ExtractorTestProcess.TestContext>(definition);
        services.AddScoped<ExtractorSweepTestProcess.ScopeCleanupOceDependency>(_
            => new ExtractorSweepTestProcess.ScopeCleanupOceDependency(throwOnDispose: created++ > 0));
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var _ = provider;
        var runner = provider.GetRequiredService<RunToCompletionRunner>();

        var exitCode = await runner.RunAsync();

        // A cleanup cancellation the host never requested on an otherwise clean run is a job
        // failure; the recorded outcomes (publication, consumption) are not lost.
        Assert.Equal(RunToCompletionExitCodes.JobFailure, exitCode);
        Assert.Single(fakes.Topic.Events);
        Assert.Empty(fakes.Files.Files);
    }

    [Fact]
    public async Task MissingDependency_FailsBeforeAnyFileIsProcessed()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var services = fakes.CreateEdgeServices();
        services.AddExtractor(ExtractorSweepTestProcess.Definition());
        services.RemoveAllKeyed<IFileAdapter>(ExtractorSweepTestProcess.SourceKey);
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var _ = provider.ConfigureAwait(false);
        var job = provider.GetRequiredService<IRunToCompletionJob>();

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => job.ExecuteAsync(CancellationToken.None));

        // The missing edge is reported once, before the batch — not per file.
        Assert.Contains(ExtractorSweepTestProcess.SourceKey, error.Message);
        Assert.Single(fakes.Files.Files); // no file was touched
        Assert.Empty(fakes.Topic.Events);
    }

    [Fact]
    public async Task SingletonJob_DoesNotCaptureAScopedPipeline()
    {
        var (provider, fakes) = BuildRegistration();
        var job = provider.GetRequiredService<IRunToCompletionJob>(); // singleton resolution must succeed

        // The scoped pipeline is not resolvable from the root provider.
        Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<Extractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>>());

        // But the single instance drives fresh per-file scopes end to end.
        fakes.Files.AddFile("order-1.json", ValidInput);
        fakes.Files.AddFile("order-2.json", ValidInput);
        var summary = await job.ExecuteAsync(CancellationToken.None);

        Assert.Equal(2, summary.Processed);
        Assert.Empty(fakes.Files.Files);
    }

    [Fact]
    public async Task SidecarStartupNotAvailable_StillFailsAsInfrastructure()
    {
        var (provider, daprClient) = BuildRegistrationWithSlowSidecar();

        var exitCode = await provider.GetRequiredService<RunToCompletionRunner>().RunAsync();

        // Existing sidecar startup failure behavior remains intact.
        Assert.Equal(RunToCompletionExitCodes.InfrastructureFailure, exitCode);
        Assert.DoesNotContain(daprClient.ReceivedCalls(), c => c.GetMethodInfo().Name == "ShutdownSidecarAsync");
    }

    [Fact]
    public async Task DirectJob_RequiresNoSidecarLifecycleCalls()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", ValidInput);
        var provider = fakes.CreateServices().BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var _ = provider;
        var daprClient = provider.GetRequiredService<DaprClient>();
        var job = provider.GetRequiredService<TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>>().CreateJob(provider, NullLoggerFactory.Instance);

        var summary = await job.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Empty(daprClient.ReceivedCalls()); // no sidecar wait, no shutdown
    }

    private static (ServiceProvider Provider, ExtractorSweepTestProcess.SweepFakes Fakes) BuildRegistration(
        Action<RunToCompletionOptions>? configureOptions = null)
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        var services = fakes.CreateEdgeServices();
        services.AddExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>(
            ExtractorSweepTestProcess.Definition(), options =>
            {
                options.SidecarTimeout = TimeSpan.FromSeconds(1);      // keep runner tests fast
                options.SidecarShutdownTimeout = TimeSpan.FromSeconds(1);
                configureOptions?.Invoke(options);
            });
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        return (provider, fakes);
    }

    private static (ServiceProvider Provider, ExtractorSweepTestProcess.SweepFakes Fakes) BuildMixedBatchRegistration()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-a.json", ValidInput);   // publishes → Processed
        fakes.Files.AddUnreadableFile("corrupt.json");     // read failure → Failed
        fakes.Files.AddFile("order-b.json", ValidInput);   // queued duplicate → Cancelled
        fakes.Idempotency.QueueStatus(new Intropy.Contracts.IdempotencyService.StatusResponse(
            Intropy.Contracts.IdempotencyService.Action.Ignore, Intropy.Contracts.IdempotencyService.Reason.SameData));
        var services = fakes.CreateEdgeServices();
        services.AddExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
            ExtractorTestProcess.TestContext>(ExtractorSweepTestProcess.Definition());
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        return (provider, fakes);
    }

    private static (ServiceProvider Provider, DaprClient DaprClient) BuildRegistrationWithSlowSidecar()
    {
        var (provider, fakes) = BuildRegistration();
        var daprClient = provider.GetRequiredService<DaprClient>();
        daprClient.WaitForSidecarAsync(Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                var ct = callInfo.Arg<CancellationToken>();
                await Task.Delay(Timeout.Infinite, ct);
            });
        return (provider, daprClient);
    }

    private static IServiceCollection RegisterFull(IServiceCollection services,
        TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext> definition)
    {
        services.AddLogging();
        services.AddTestIdentity();
        services.AddScoped<ExtractorTestProcess.ScopedDependency>();
        services.AddKeyedSingleton<IFileAdapter>(ExtractorSweepTestProcess.SourceKey, new InMemoryFileAdapter());
        services.AddSingleton(Substitute.For<DaprClient>());
        services.AddExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output, ExtractorTestProcess.TestContext>(
            definition, options => options.JobName = "sweep-source");
        return services;
    }
}
