using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// When a claim-check configuration cannot produce a policy, the run fails before it touches a
/// source file.
/// </summary>
/// <remarks>
/// Worth pinning rather than assuming: the threshold is the broker's maximum message size, which
/// the framework cannot know, so a registered store in <c>Auto</c> with no threshold has no safe
/// default and must refuse. Where that refusal lands is the whole point — the sweep composes each
/// port's pipeline once in <c>ValidateAsync</c> before listing anything, so the job dies on
/// configuration rather than consuming files and failing each one, which would leave a half-swept
/// source and a run that looked like it had work to do.
/// </remarks>
public class ExtractorClaimCheckStartupTests
{
    private const string Input =
        """{"Id":"input-42","Version":"2026-01-15T09:30:00+00:00","Valid":true}""";

    private static ExtractorJob<ExtractorTestProcess.TestContext> JobWithClaimStore(
        ExtractorSweepTestProcess.SweepFakes fakes, Action<ExtractorOptions>? configureOptions)
    {
        var services = fakes.CreateServices(
            definition: ExtractorSweepTestProcess.Definition() with { ConfigureOptions = configureOptions },
            configure: s => s.AddKeyedSingleton<IFileAdapter>(
                ClaimStoreServiceCollectionExtensions.ClaimStoreKey, new InMemoryFileAdapter()));
        var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        return provider
            .GetRequiredService<TestExtractor<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
                ExtractorTestProcess.TestContext>>()
            .CreateJob(provider, NullLoggerFactory.Instance);
    }

    // Auto is the default mode, so "a store is registered and nobody said above what size" is the
    // shape of an unfinished configuration rather than an unusual one.
    [Fact]
    public async Task AStoreInAutoModeWithNoThreshold_FailsTheRun_WithoutReadingAnyFile()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", Input);
        var job = JobWithClaimStore(fakes, configureOptions: null);

        var error = await Assert.ThrowsAnyAsync<Exception>(() => job.ExecuteAsync(CancellationToken.None));

        Assert.Contains(nameof(ExtractorOptions.ClaimCheckThresholdBytes), Flatten(error),
            StringComparison.Ordinal);
        // The file is still there and nothing was published: the run refused before it swept.
        Assert.True(fakes.Files.Files.ContainsKey("order-1.json"));
        Assert.Empty(fakes.Topic.Events);
    }

    // The control: give it a threshold and the same registration sweeps normally.
    [Fact]
    public async Task AStoreWithAThreshold_SweepsNormally()
    {
        var fakes = new ExtractorSweepTestProcess.SweepFakes();
        fakes.Files.AddFile("order-1.json", Input);
        var job = JobWithClaimStore(fakes, o => o.ClaimCheckThresholdBytes = 100_000);

        var summary = await job.ExecuteAsync(CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Single(fakes.Topic.Events);
    }

    private static string Flatten(Exception error)
    {
        var text = error.ToString();
        return error is AggregateException aggregate
            ? text + string.Join('\n', aggregate.Flatten().InnerExceptions.Select(e => e.ToString()))
            : text;
    }
}
