using Dapr.Client;
using Intropy.Framework.Hosting.RunToCompletion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.RunToCompletion;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddRunToCompletionJob_ShouldThrowArgumentNullException_WhenServicesIsNull()
    {
        // Verifies that null service collection is rejected
        IServiceCollection? services = null;

        var exception = Assert.Throws<ArgumentNullException>(() =>
            services!.AddRunToCompletionJob<TestJob>(_ => { }));

        Assert.Equal("services", exception.ParamName);
    }

    [Fact]
    public void AddRunToCompletionJob_ShouldThrowArgumentNullException_WhenConfigureOptionsIsNull()
    {
        // Verifies that null configuration action is rejected
        var services = GetServices();

        var exception = Assert.Throws<ArgumentNullException>(() =>
            services.AddRunToCompletionJob<TestJob>(null!));

        Assert.Equal("configureOptions", exception.ParamName);
    }

    [Fact]
    public void AddRunToCompletionJob_ShouldThrowInvalidOperationException_WhenJobNameIsNotConfigured()
    {
        // Verifies that JobName is required
        var services = GetServices();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddRunToCompletionJob<TestJob>(_ =>
            {
                // JobName intentionally not set
            }));

        Assert.Equal("JobName must be configured.", exception.Message);
    }

    [Fact]
    public void AddRunToCompletionJob_ShouldRegisterOptions_WithConfiguredValues()
    {
        // Verifies that options are registered with the configured values
        var services = GetServices();

        services.AddRunToCompletionJob<TestJob>(options =>
        {
            options.JobName = "my-job";
            options.SidecarTimeoutSeconds = 15;
            options.SidecarShutdownTimeoutSeconds = 5;
        });

        var provider = services.BuildServiceProvider();
        var registeredOptions = provider.GetRequiredService<RunToCompletionOptions>();

        Assert.Equal("my-job", registeredOptions.JobName);
        Assert.Equal(15, registeredOptions.SidecarTimeoutSeconds);
        Assert.Equal(5, registeredOptions.SidecarShutdownTimeoutSeconds);
    }

    [Fact]
    public void AddRunToCompletionJob_ShouldRegisterJob_WhenConsumerDidNot()
    {
        // Verifies that the framework registers TJob itself, so the caller does not have to
        var services = GetServices();

        services.AddRunToCompletionJob<TestJob>(options => options.JobName = "my-job");

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IRunToCompletionJob>();

        Assert.IsType<TestJob>(resolved);
    }

    [Fact]
    public void AddRunToCompletionJob_ShouldResolveJob_FromConsumerRegistration()
    {
        // Verifies that an explicit consumer registration (e.g. a pre-built instance) wins
        var services = GetServices();
        var job = new TestJob();
        services.AddSingleton<TestJob>(job);

        services.AddRunToCompletionJob<TestJob>(options => options.JobName = "my-job");

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IRunToCompletionJob>();

        Assert.Same(job, resolved);
    }

    [Fact]
    public void AddRunToCompletionJob_ShouldRegisterRunner_WhenAllDependenciesArePresent()
    {
        // Verifies that the runner is successfully registered when all dependencies exist
        var services = GetServices();
        services.AddSingleton(Substitute.For<ILoggerFactory>());

        services.AddRunToCompletionJob<TestJob>(options => options.JobName = "my-job");

        var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredService<RunToCompletionRunner>();

        Assert.NotNull(runner);
    }

    [Fact]
    public void AddRunToCompletionJob_ShouldReturnServiceCollection_ForMethodChaining()
    {
        // Verifies that the method returns the service collection for fluent API
        var services = GetServices();

        var result = services.AddRunToCompletionJob<TestJob>(options => options.JobName = "my-job");

        Assert.Same(services, result);
    }

    private static ServiceCollection GetServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_ => new DaprClientBuilder().Build());
        return services;
    }

    public class TestJob : IRunToCompletionJob
    {
        public virtual Task<JobRunSummary> ExecuteAsync(CancellationToken ct) =>
            Task.FromResult(JobRunSummary.Empty);
    }
}
