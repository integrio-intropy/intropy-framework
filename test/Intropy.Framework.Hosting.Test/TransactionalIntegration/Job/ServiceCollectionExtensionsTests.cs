using Dapr.Client;
using Dapr.Messaging.PublishSubscribe.Extensions;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.TransactionalIntegration.Job;
using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Job;

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
    public void
        AddTransactionalIntegration_ShouldThrowInvalidOperationException_WhenDaprPubSubNameIsNotConfigured()
    {
        // Verifies that DaprPubSubName is required
        var services = GetServices();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddTransactionalIntegration(options =>
            {
                options.DaprTopicName = "test-topic";
                // DaprPubSubName intentionally not set
            }));

        Assert.Equal("DaprPubSubName must be configured.", exception.Message);
    }

    [Fact]
    public void
        AddTransactionalIntegration_ShouldThrowInvalidOperationException_WhenDaprTopicNameIsNotConfigured()
    {
        // Verifies that DaprTopicName is required
        var services = GetServices();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddTransactionalIntegration(options =>
            {
                options.DaprPubSubName = "test-pubsub";
                // DaprTopicName intentionally not set
            }));

        Assert.Equal("DaprTopicName must be configured.", exception.Message);
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
            options.IdleTimeoutSeconds = 10;
            options.PostIdleGracePeriodSeconds = 30;
            options.MaxMessageProcessingTimeSeconds = 20;
        });

        var provider = services.BuildServiceProvider();
        var registeredOptions = provider.GetRequiredService<TransactionalIntegrationOptions>();

        Assert.Equal("my-pubsub", registeredOptions.DaprPubSubName);
        Assert.Equal("my-topic", registeredOptions.DaprTopicName);
        Assert.Equal(10, registeredOptions.IdleTimeoutSeconds);
        Assert.Equal(30, registeredOptions.PostIdleGracePeriodSeconds);
        Assert.Equal(20, registeredOptions.MaxMessageProcessingTimeSeconds);
    }

    [Fact]
    public void AddTransactionalIntegration_ShouldRegisterITopicSubscriber()
    {
        // Verifies that ITopicSubscriber is registered as DaprTopicSubscriber
        var services = GetServices();

        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        var provider = services.BuildServiceProvider();
        var topicSubscriber = provider.GetRequiredService<ITopicSubscriber>();

        Assert.NotNull(topicSubscriber);
        Assert.IsType<DaprTopicSubscriber>(topicSubscriber);
    }

    [Fact]
    public void AddTransactionalIntegration_ShouldRegisterLifecycle_WhenAllDependenciesArePresent()
    {
        // Verifies that lifecycle is successfully registered when all dependencies exist
        var services = GetServices();
        services.AddSingleton(Substitute.For<IFileAdapter>());
        services.AddSingleton(Substitute.For<ILoggerFactory>());
        services.AddSingleton(Substitute.For<ISendPipeline<Context>>());
        services.AddSingleton(Substitute.For<ISourceLister>());
        services.AddSingleton(Substitute.For<IReceivePipeline<Context>>());

        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        var provider = services.BuildServiceProvider();
        var lifecycle = provider.GetRequiredService<TransactionalIntegrationLifecycle>();

        Assert.NotNull(lifecycle);
    }

    [Fact]
    public void AddTransactionalIntegration_ShouldRegisterRunner_WhenAllDependenciesArePresent()
    {
        // Verifies that runner is successfully registered when all dependencies exist
        var services = GetServices();
        services.AddSingleton(Substitute.For<IFileAdapter>());
        services.AddSingleton(Substitute.For<ILoggerFactory>());
        services.AddSingleton(Substitute.For<ISendPipeline<Context>>());
        services.AddSingleton(Substitute.For<ISourceLister>());
        services.AddSingleton(Substitute.For<IReceivePipeline<Context>>());

        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredService<TransactionalIntegrationRunner>();

        Assert.NotNull(runner);
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
        services.AddDaprPubSubClient();
        return services;
    }
}
