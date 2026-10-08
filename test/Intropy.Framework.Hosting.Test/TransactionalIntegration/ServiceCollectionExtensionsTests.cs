using Intropy.Framework.Testing.Topics;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddTransactionalIntegration_ShouldThrowArgumentNullException_WhenServicesIsNull()
    {
        // Verifies that null service collection is rejected
        IServiceCollection? services = null;

        var exception = Assert.Throws<ArgumentNullException>(() =>
            services!.AddTransactionalIntegration(_ => { }));

        Assert.Equal("services", exception.ParamName);
    }

    [Fact]
    public void AddTransactionalIntegration_ShouldThrowArgumentNullException_WhenConfigureOptionsIsNull()
    {
        // Verifies that null configuration action is rejected
        var services = GetServices();

        var exception = Assert.Throws<ArgumentNullException>(() =>
            services.AddTransactionalIntegration(null!));

        Assert.Equal("configureOptions", exception.ParamName);
    }

    [Fact]
    public void AddTransactionalIntegration_DefaultsUnsetNames_ToTheInternalHopTheTopologyGenerates()
    {
        var services = GetServices();

        services.AddTransactionalIntegration(_ => { });

        var options = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("internal-orders-integration", options.DaprPubSubName);
        Assert.Equal("hop", options.DaprTopicName);
        // Both shapes read the defaults.
        Assert.Equal("internal-orders-integration", options.Subscription.PubSubName);
        Assert.Equal("hop", options.Subscription.TopicName);
    }

    [Fact]
    public void AddTransactionalIntegration_DefaultsOnlyTheUnsetName()
    {
        var services = GetServices();

        services.AddTransactionalIntegration(options => options.DaprTopicName = "test-topic");

        var options = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("internal-orders-integration", options.DaprPubSubName);
        Assert.Equal("test-topic", options.DaprTopicName);
    }

    [Fact]
    public void AddTransactionalIntegration_NamesTheDefaultPubSubAfterTheAppId_ForADottedComponentName()
    {
        // The topology names the hop after the app id: the component name with dots as dashes.
        var services = new ServiceCollection();
        services.AddSingleton(_ => new DaprClientBuilder().Build());
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "int1055.order-sync";
            options.ServiceNamespace = "example";
        });

        services.AddTransactionalIntegration(_ => { });

        var options = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("internal-int1055-order-sync", options.DaprPubSubName);
    }

    [Fact]
    public void AddTransactionalIntegration_ShouldRegisterOptions_WithConfiguredValues()
    {
        // Verifies that options are registered with the configured values
        var services = GetServices();

        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "my-pubsub";
            options.DaprTopicName = "my-topic";
            options.IdleTimeout = TimeSpan.FromSeconds(10);
            options.PostIdleGracePeriod = TimeSpan.FromSeconds(30);
            options.MaxMessageProcessingTime = TimeSpan.FromSeconds(20);
        });

        var provider = services.BuildServiceProvider();
        var registeredOptions = provider.GetRequiredService<TransactionalIntegrationOptions>();

        Assert.Equal("my-pubsub", registeredOptions.DaprPubSubName);
        Assert.Equal("my-topic", registeredOptions.DaprTopicName);
        Assert.Equal(TimeSpan.FromSeconds(10), registeredOptions.IdleTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), registeredOptions.PostIdleGracePeriod);
        Assert.Equal(TimeSpan.FromSeconds(20), registeredOptions.MaxMessageProcessingTime);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(65536)]
    public void AddTransactionalIntegration_WithAnInvalidCallbackPort_FailsAtRegistration(int port)
    {
        var services = GetServices();

        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
            options.CallbackPort = port;
        }));
    }

    [Fact]
    public void AddTransactionalIntegration_ShouldRegisterLifecycle_WhenAllDependenciesArePresent()
    {
        // Verifies that lifecycle is successfully registered when all dependencies exist
        var services = GetServices();
        services.AddSingleton(Substitute.For<IFileAdapter>());
        services.AddSingleton(Substitute.For<ILoggerFactory>());
        services.AddSingleton(Substitute.For<ISendPipeline<Context>>());
        services.AddSingleton(Substitute.For<IReceivePipeline<Context>>());

        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        var provider = services.BuildServiceProvider();
        var lifecycle = provider.GetRequiredService<TransactionalIntegrationJob<Context>>();

        Assert.NotNull(lifecycle);
    }

    [Fact]
    public void AddTransactionalIntegration_ShouldHostTheLifecycleOnTheJobRunner()
    {
        // Verifies that the lifecycle is the run-to-completion job and the shared runner hosts it
        var services = GetServices();
        services.AddSingleton(Substitute.For<IFileAdapter>());
        services.AddSingleton(Substitute.For<ILoggerFactory>());
        services.AddSingleton(Substitute.For<ISendPipeline<Context>>());
        services.AddSingleton(Substitute.For<IReceivePipeline<Context>>());

        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<JobRunner>());
        Assert.Same(provider.GetRequiredService<TransactionalIntegrationJob<Context>>(),
            provider.GetRequiredService<IJob>());
        Assert.Equal("orders-integration", provider.GetRequiredService<JobOptions>().JobName);
    }

    [Fact]
    public void AddTransactionalIntegration_AppliesRunnerSettings()
    {
        // Verifies that the job name and sidecar timeouts are configurable through the runner options
        var services = GetServices();

        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        }, job =>
        {
            job.JobName = "orders-ti";
            job.SidecarTimeout = TimeSpan.FromSeconds(5);
        });

        var jobOptions = services.BuildServiceProvider().GetRequiredService<JobOptions>();
        Assert.Equal("orders-ti", jobOptions.JobName);
        Assert.Equal(TimeSpan.FromSeconds(5), jobOptions.SidecarTimeout);
    }

    [Fact]
    public async Task AddTransactionalIntegration_RegistersAReceivePipelineThatPublishesThroughTheRegisteredEnqueueStep()
    {
        // Verifies that the receive side needs no component code: the default receive pipeline
        // publishes through the registered EnqueueStep, which is how tests swap in a fake
        var services = GetServices();
        services.AddLogging();
        var fake = new FakeEnqueueStep<Context>(new FrameworkOptions { ComponentName = "orders-integration", ServiceNamespace = "example" });
        services.AddSingleton<EnqueueStep<Context>>(fake);
        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });
        var provider = services.BuildServiceProvider();

        var pipeline = provider.GetRequiredService<IReceivePipeline<Context>>();
        var (result, _) = await pipeline.Execute(new SourceItem("order-1.json", "{}"u8.ToArray()),
            new Context(new Dictionary<string, string>()));

        Assert.IsType<StepResult<SourceItem>.Success>(result);
        Assert.Equal("order-1.json", Assert.Single(fake.Captured).Item.Id);
    }

    [Fact]
    public void AddTransactionalIntegration_DefaultsTheEnqueueStepToTheDaprTopicEnqueuer()
    {
        var services = GetServices();
        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        var provider = services.BuildServiceProvider();

        Assert.IsType<DaprTopicEnqueuer<Context>>(provider.GetRequiredService<EnqueueStep<Context>>());
    }

    [Fact]
    public void AddTransactionalIntegration_KeepsAReceivePipelineTheComponentRegistered()
    {
        var services = GetServices();
        var custom = Substitute.For<IReceivePipeline<Context>>();
        services.AddSingleton(custom);
        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        var provider = services.BuildServiceProvider();

        Assert.Same(custom, provider.GetRequiredService<IReceivePipeline<Context>>());
    }

    [Fact]
    public void AddTransactionalIntegration_ShouldReturnServiceCollection_ForMethodChaining()
    {
        // Verifies that the method returns the service collection for fluent API
        var services = GetServices();

        var result = services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        Assert.Same(services, result);
    }

    private static ServiceCollection GetServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_ => new DaprClientBuilder().Build());
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "orders-integration";
            options.ServiceNamespace = "example";
        });
        return services;
    }
}
