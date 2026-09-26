using Dapr.Client;
using Intropy.Framework.Core.Configuration;
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
    public void AddRunToCompletionJob_DefaultsTheJobNameToTheComponentName()
    {
        // Verifies that every job is named after its component unless JobName overrides it
        var services = GetServices();
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "orders-import";
            options.ServiceNamespace = "example";
        });

        services.AddRunToCompletionJob<TestJob>();

        var provider = services.BuildServiceProvider();
        Assert.Equal("orders-import", provider.GetRequiredService<RunToCompletionOptions>().JobName);
    }

    [Fact]
    public void AddRunToCompletionJob_WithoutJobNameOrComponentName_FailsWhenResolved()
    {
        // Verifies that a job never runs unnamed
        var services = GetServices();
        services.AddRunToCompletionJob<TestJob>();

        var provider = services.BuildServiceProvider();
        var exception = Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<RunToCompletionOptions>());

        Assert.Contains("AddIntropyFramework", exception.Message);
    }

    [Fact]
    public void AddRunToCompletionJob_ShouldRegisterOptions_WithConfiguredValues()
    {
        // Verifies that options are registered with the configured values
        var services = GetServices();

        services.AddRunToCompletionJob<TestJob>(options =>
        {
            options.JobName = "my-job";
            options.SidecarTimeout = TimeSpan.FromSeconds(15);
            options.SidecarShutdownTimeout = TimeSpan.FromSeconds(5);
        });

        var provider = services.BuildServiceProvider();
        var registeredOptions = provider.GetRequiredService<RunToCompletionOptions>();

        Assert.Equal("my-job", registeredOptions.JobName);
        Assert.Equal(TimeSpan.FromSeconds(15), registeredOptions.SidecarTimeout);
        Assert.Equal(TimeSpan.FromSeconds(5), registeredOptions.SidecarShutdownTimeout);
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
