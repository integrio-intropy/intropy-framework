using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Core.Test.Configuration;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddIntropyFramework_ShouldUseFunc()
    {
        // Arrange
        Environment.SetEnvironmentVariable(Constants.ComponentNameEnvironmentVariable, "FROMENV");
        Environment.SetEnvironmentVariable(Constants.ServiceNamespaceEnvironmentVariable, "FROMENV");
        const string expected = "FROMFUNC";
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddIntropyFramework(opts =>
        {
            opts.ComponentName = expected;
            opts.ServiceNamespace = expected;
        }); 
        var serviceProvider = serviceCollection.BuildServiceProvider();

        // Act
        var frameworkOptions = serviceProvider.GetService<FrameworkOptions>();
        
        // Assert
        Assert.Equal(expected, frameworkOptions!.ComponentName);
        Assert.Equal(expected, frameworkOptions.ServiceNamespace);
    }
    
    [Fact]
    public void AddIntropyFramework_ShouldUseEnv()
    {
        // Arrange
        const string expected = "FROMENV";
        Environment.SetEnvironmentVariable(Constants.ComponentNameEnvironmentVariable, expected);
        Environment.SetEnvironmentVariable(Constants.ServiceNamespaceEnvironmentVariable, expected);
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddIntropyFramework();
        var serviceProvider = serviceCollection.BuildServiceProvider();

        // Act
        var frameworkOptions = serviceProvider.GetService<FrameworkOptions>();
        
        // Assert
        Assert.Equal(expected, frameworkOptions!.ComponentName);
        Assert.Equal(expected, frameworkOptions.ServiceNamespace);
    }
    
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void AddIntropyFramework_ShouldThrow_WhenComponentNameIsNullOrEmpty(string? input)
    {
        // Arrange
        Environment.SetEnvironmentVariable(Constants.ComponentNameEnvironmentVariable, input);
        Environment.SetEnvironmentVariable(Constants.ServiceNamespaceEnvironmentVariable, "abc");
        var serviceCollection = new ServiceCollection();
        
        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => serviceCollection.AddIntropyFramework());
    }

    [Fact]
    public void AddIntropyFramework_ReadsThePubSubNamesFromEnv_AlongsideTheFunc()
    {
        // Code sets the component's own name; the pub/subs it uses are the deployment's to name.
        Environment.SetEnvironmentVariable(Constants.SystemPubSubNameEnvironmentVariable, "order-flow-pubsub");
        Environment.SetEnvironmentVariable(Constants.InternalQueuePubSubNameEnvironmentVariable, "order-flow-queue");
        try
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddIntropyFramework(opts =>
            {
                opts.ComponentName = "orders";
                opts.ServiceNamespace = "example";
            });

            var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

            Assert.Equal("order-flow-pubsub", frameworkOptions.SystemPubSubName);
            Assert.Equal("order-flow-queue", frameworkOptions.InternalQueuePubSubName);
        }
        finally
        {
            Environment.SetEnvironmentVariable(Constants.SystemPubSubNameEnvironmentVariable, null);
            Environment.SetEnvironmentVariable(Constants.InternalQueuePubSubNameEnvironmentVariable, null);
        }
    }

    [Fact]
    public void AddIntropyFramework_PrefersThePubSubNamesFromEnv_OverTheFunc()
    {
        // The deployment names the components it renders; a name in code must not pin them.
        Environment.SetEnvironmentVariable(Constants.SystemPubSubNameEnvironmentVariable, "bus-from-env");
        Environment.SetEnvironmentVariable(Constants.InternalQueuePubSubNameEnvironmentVariable, "queue-from-env");
        try
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddIntropyFramework(opts =>
            {
                opts.ComponentName = "orders";
                opts.ServiceNamespace = "example";
                opts.SystemPubSubName = "bus-from-func";
                opts.InternalQueuePubSubName = "queue-from-func";
            });

            var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

            Assert.Equal("bus-from-env", frameworkOptions.SystemPubSubName);
            Assert.Equal("queue-from-env", frameworkOptions.InternalQueuePubSubName);
        }
        finally
        {
            Environment.SetEnvironmentVariable(Constants.SystemPubSubNameEnvironmentVariable, null);
            Environment.SetEnvironmentVariable(Constants.InternalQueuePubSubNameEnvironmentVariable, null);
        }
    }

    [Fact]
    public void AddIntropyFramework_FallsBackToThePubSubNamesTheFuncSets_WhenEnvIsUnset()
    {
        Environment.SetEnvironmentVariable(Constants.SystemPubSubNameEnvironmentVariable, null);
        Environment.SetEnvironmentVariable(Constants.InternalQueuePubSubNameEnvironmentVariable, null);
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddIntropyFramework(opts =>
        {
            opts.ComponentName = "orders";
            opts.ServiceNamespace = "example";
            opts.SystemPubSubName = "bus-from-func";
            opts.InternalQueuePubSubName = "queue-from-func";
        });

        var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

        Assert.Equal("bus-from-func", frameworkOptions.SystemPubSubName);
        Assert.Equal("queue-from-func", frameworkOptions.InternalQueuePubSubName);
    }

    [Fact]
    public void AddIntropyFramework_LeavesThePubSubNamesUnset_WhenNeitherSetsThem()
    {
        Environment.SetEnvironmentVariable(Constants.SystemPubSubNameEnvironmentVariable, null);
        Environment.SetEnvironmentVariable(Constants.InternalQueuePubSubNameEnvironmentVariable, null);
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddIntropyFramework(opts =>
        {
            opts.ComponentName = "orders";
            opts.ServiceNamespace = "example";
        });

        var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

        Assert.Null(frameworkOptions.SystemPubSubName);
        Assert.Null(frameworkOptions.InternalQueuePubSubName);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void AddIntropyFramework_ShouldThrow_WhenServiceNamespaceIsNullOrEmpty(string? input)
    {
        // Arrange
        Environment.SetEnvironmentVariable(Constants.ComponentNameEnvironmentVariable, "abc");
        Environment.SetEnvironmentVariable(Constants.ServiceNamespaceEnvironmentVariable, input);
        var serviceCollection = new ServiceCollection();

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => serviceCollection.AddIntropyFramework());
    }
}
