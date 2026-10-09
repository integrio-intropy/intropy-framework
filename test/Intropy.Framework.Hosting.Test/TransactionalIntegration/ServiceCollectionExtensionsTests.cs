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
using Microsoft.Extensions.Configuration;
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
    public void AddTransactionalIntegration_DefaultsUnsetNames_ToTheInternalQueueTheTopologyGenerates()
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
        // The topology names the internal queue after the app id: the component name with dots as dashes.
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

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(-10000L)]
    [InlineData(long.MaxValue)]
    [InlineData(42949672950000L)]
    public void InvalidInternalQueueReadyTimeout_FailsAtRegistrationInBothShapes(long ticks)
    {
        var timeout = TimeSpan.FromTicks(ticks);
        var byOptions = GetServices();
        var optionsError = Assert.Throws<ArgumentOutOfRangeException>(() =>
            byOptions.AddTransactionalIntegration(options => options.InternalQueueReadyTimeout = timeout));
        Assert.Contains("TransactionalIntegrationOptions.InternalQueueReadyTimeout", optionsError.Message);
        // A rejected registration must not prevent a corrected retry.
        byOptions.AddTransactionalIntegration(options => options.InternalQueueReadyTimeout = TimeSpan.FromSeconds(1));

        var byDefinition = GetServices();
        var definitionError = Assert.Throws<ArgumentOutOfRangeException>(() =>
            byDefinition.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>
            {
                Configure = options => options.InternalQueueReadyTimeout = timeout
            }));
        Assert.Contains("TransactionalIntegrationOptions.InternalQueueReadyTimeout", definitionError.Message);
        byDefinition.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>());
    }

    [Fact]
    public void ValidInternalQueueReadyTimeout_FlowsThroughConfiguration()
    {
        var services = GetServices();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["InternalQueueReadyTimeout"] = "00:00:07" })
            .Build();

        services.AddTransactionalIntegration(configuration, () => new TransactionalIntegrationDefinition<Context>());

        using var provider = services.BuildServiceProvider();
        Assert.Equal(TimeSpan.FromSeconds(7),
            provider.GetRequiredService<TransactionalIntegrationOptions>().InternalQueueReadyTimeout);
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
