using Dapr.Client;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.Jobs;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddJob_ShouldThrowArgumentNullException_WhenServicesIsNull()
    {
        // Verifies that null service collection is rejected
        IServiceCollection? services = null;

        var exception = Assert.Throws<ArgumentNullException>(() =>
            services!.AddJob<TestJob>(_ => { }));

        Assert.Equal("services", exception.ParamName);
    }

    [Fact]
    public void AddJob_DefaultsTheJobNameToTheComponentName()
    {
        // Verifies that every job is named after its component unless JobName overrides it
        var services = GetServices();
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "orders-import";
            options.ServiceNamespace = "example";
        });

        services.AddJob<TestJob>();

        var provider = services.BuildServiceProvider();
        Assert.Equal("orders-import", provider.GetRequiredService<JobOptions>().JobName);
    }

    [Fact]
    public void AddJob_RejectsASecondJob()
    {
        // Two runners would race for the process's exit code and sidecar; a host is one job.
        var services = GetServices();

        services.AddJob<TestJob>();

        Assert.Throws<InvalidOperationException>(() => services.AddJob<TestJob>());
    }

    [Fact]
    public void AddJob_WithoutJobNameOrComponentName_FailsWhenResolved()
    {
        // Verifies that a job never runs unnamed
        var services = GetServices();
        services.AddJob<TestJob>();

        var provider = services.BuildServiceProvider();
        var exception = Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<JobOptions>());

        Assert.Contains("AddIntropyFramework", exception.Message);
    }

    [Fact]
    public void AddJob_ShouldRegisterOptions_WithConfiguredValues()
    {
        // Verifies that options are registered with the configured values
        var services = GetServices();

        services.AddJob<TestJob>(options =>
        {
            options.JobName = "my-job";
            options.SidecarTimeout = TimeSpan.FromSeconds(15);
            options.SidecarShutdownTimeout = TimeSpan.FromSeconds(5);
        });

        var provider = services.BuildServiceProvider();
        var registeredOptions = provider.GetRequiredService<JobOptions>();

        Assert.Equal("my-job", registeredOptions.JobName);
        Assert.Equal(TimeSpan.FromSeconds(15), registeredOptions.SidecarTimeout);
        Assert.Equal(TimeSpan.FromSeconds(5), registeredOptions.SidecarShutdownTimeout);
    }

    [Fact]
    public void AddJob_ShouldRegisterJob_WhenConsumerDidNot()
    {
        // Verifies that the framework registers TJob itself, so the caller does not have to
        var services = GetServices();

        services.AddJob<TestJob>(options => options.JobName = "my-job");

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IJob>();

        Assert.IsType<TestJob>(resolved);
    }

    [Fact]
    public void AddJob_ShouldResolveJob_FromConsumerRegistration()
    {
        // Verifies that an explicit consumer registration (e.g. a pre-built instance) wins
        var services = GetServices();
        var job = new TestJob();
        services.AddSingleton<TestJob>(job);

        services.AddJob<TestJob>(options => options.JobName = "my-job");

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IJob>();

        Assert.Same(job, resolved);
    }

    [Fact]
    public void AddJob_ShouldRegisterRunner_WhenAllDependenciesArePresent()
    {
        // Verifies that the runner is successfully registered when all dependencies exist
        var services = GetServices();
        services.AddSingleton(Substitute.For<ILoggerFactory>());

        services.AddJob<TestJob>(options => options.JobName = "my-job");

        var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredService<JobRunner>();

        Assert.NotNull(runner);
    }

    [Fact]
    public void AddJob_ShouldReturnServiceCollection_ForMethodChaining()
    {
        // Verifies that the method returns the service collection for fluent API
        var services = GetServices();

        var result = services.AddJob<TestJob>(options => options.JobName = "my-job");

        Assert.Same(services, result);
    }

    private static ServiceCollection GetServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_ => new DaprClientBuilder().Build());
        return services;
    }

    public class TestJob : IJob
    {
        public virtual Task<RunSummary> ExecuteAsync(CancellationToken ct) =>
            Task.FromResult(RunSummary.Empty);
    }
}
