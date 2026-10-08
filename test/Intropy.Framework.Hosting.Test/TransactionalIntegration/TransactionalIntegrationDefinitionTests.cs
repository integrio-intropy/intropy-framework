using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

/// <summary>
/// The <see cref="TransactionalIntegrationDefinition{TCtx}"/> registration: validated at registration
/// (missing pub/sub, topic; out-of-range callback port) so misconfiguration fails at startup with
/// the member that is wrong — replacing the bare "must be configured." errors — registering the
/// declared source port, defaulting the context factory for the base Context, and flowing the
/// runner settings.
/// </summary>
public class TransactionalIntegrationDefinitionTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void UnsetNames_DefaultToTheInternalHopTheTopologyGenerates(string? name)
    {
        var services = GetServices();

        services.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>
        {
            DaprPubSubName = name,
            DaprTopicName = name,
        });

        var options = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("internal-orders-integration", options.DaprPubSubName);
        Assert.Equal("hop", options.DaprTopicName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(65536)]
    public void OutOfRangeCallbackPort_FailsAtRegistration(int port)
    {
        var services = GetServices();

        var error = Assert.Throws<ArgumentOutOfRangeException>(() =>
            services.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>
            {
                DaprPubSubName = "test-pubsub",
                DaprTopicName = "orders.received",
                Configure = options => options.CallbackPort = port,
            }));

        Assert.Contains("TransactionalIntegrationOptions.CallbackPort must be a port number (1-65535)", error.Message);
    }

    [Fact]
    public void RegistersEquivalentlyToTheOptionsOverload()
    {
        Action<JobOptions>? configureJob = job => job.JobName = "orders-ti";
        ContextFactory<Context> contextFactory = (metadata, isRetry) => new Context(metadata, isRetry);

        var byOptions = BaseServices();
        byOptions.AddTransactionalIntegration(contextFactory, options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "orders.received";
        }, configureJob);
        var byDefinition = BaseServices();
        byDefinition.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>
        {
            DaprPubSubName = "test-pubsub",
            DaprTopicName = "orders.received",
            ContextFactory = contextFactory,
            ConfigureJob = configureJob,
        });

        // Same registrations, in the same order, with the same lifetimes — the definition is the
        // options overload's parameters plus validated sugar at registration, not a second model.
        Assert.Equal(
            byOptions.Select(d => (d.ServiceType, d.Lifetime)).ToArray(),
            byDefinition.Select(d => (d.ServiceType, d.Lifetime)).ToArray());
    }

    [Fact]
    public void RegistersTheConfiguredOptions()
    {
        var services = GetServices();

        services.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>
        {
            DaprPubSubName = "my-pubsub",
            DaprTopicName = "my-topic",
            Configure = options =>
            {
                options.IdleTimeout = TimeSpan.FromSeconds(10);
                options.PostIdleGracePeriod = TimeSpan.FromSeconds(30);
                options.MaxMessageProcessingTime = TimeSpan.FromSeconds(20);
            },
        });

        var registered = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("my-pubsub", registered.DaprPubSubName);
        Assert.Equal("my-topic", registered.DaprTopicName);
        Assert.Equal(TimeSpan.FromSeconds(10), registered.IdleTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), registered.PostIdleGracePeriod);
        Assert.Equal(TimeSpan.FromSeconds(20), registered.MaxMessageProcessingTime);
    }

    [Fact]
    public void DeclaredSourcePort_IsRegistered()
    {
        var services = GetServices();
        var completion = FileCompletion.Delete;

        services.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>
        {
            DaprPubSubName = "test-pubsub",
            DaprTopicName = "orders.received",
            SourcePort = "orders-inbox",
            Completion = completion,
        });

        using var provider = services.BuildServiceProvider();
        var sourcePort = provider.GetRequiredService<SourcePort>();
        Assert.Equal("orders-inbox", sourcePort.Name);
        Assert.Same(completion, sourcePort.Completion);
    }

    [Fact]
    public void BaseContext_RegistersWithoutACustomContextFactory()
    {
        var services = BaseServices();

        services.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>
        {
            DaprPubSubName = "test-pubsub",
            DaprTopicName = "orders.received",
        });

        using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetRequiredService<TransactionalIntegrationJob<Context>>());
    }

    [Fact]
    public void PubSubNameAndTopicName_WinOverConfigure()
    {
        var services = GetServices();

        services.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>
        {
            DaprPubSubName = "test-pubsub",
            DaprTopicName = "orders.received",
            Configure = options =>
            {
                options.DaprPubSubName = "other";
                options.DaprTopicName = "other-topic";
            },
        });

        var registered = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("test-pubsub", registered.DaprPubSubName);
        Assert.Equal("orders.received", registered.DaprTopicName);
    }

    [Fact]
    public void ConfigureJob_AppliesRunnerSettings()
    {
        var services = GetServices();

        services.AddTransactionalIntegration(new TransactionalIntegrationDefinition<Context>
        {
            DaprPubSubName = "test-pubsub",
            DaprTopicName = "orders.received",
            ConfigureJob = job =>
            {
                job.JobName = "orders-ti";
                job.SidecarTimeout = TimeSpan.FromSeconds(7);
            },
        });

        var options = services.BuildServiceProvider().GetRequiredService<JobOptions>();
        Assert.Equal("orders-ti", options.JobName);
        Assert.Equal(TimeSpan.FromSeconds(7), options.SidecarTimeout);
    }

    private static ServiceCollection GetServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_ => new DaprClientBuilder().Build());
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "orders-integration";
            options.ServiceNamespace = "example";
        });
        return services;
    }

    private static ServiceCollection BaseServices()
    {
        var services = GetServices();
        services.AddSingleton(Substitute.For<IFileAdapter>());
        services.AddLogging();
        services.AddSingleton(Substitute.For<ISendPipeline<Context>>());
        services.AddSingleton(Substitute.For<IReceivePipeline<Context>>());
        return services;
    }
}
