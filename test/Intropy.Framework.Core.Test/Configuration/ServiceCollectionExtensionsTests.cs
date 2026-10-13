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
    public void AddIntropyFramework_ReadsTheSystemNameFromEnv_AlongsideTheFunc()
    {
        // Code sets the component's own name; the system it runs in is the deployment's to say.
        Environment.SetEnvironmentVariable(Constants.SystemNameEnvironmentVariable, "order-flow");
        try
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddIntropyFramework(opts =>
            {
                opts.ComponentName = "orders";
                opts.ServiceNamespace = "example";
            });

            var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

            Assert.Equal("order-flow", frameworkOptions.SystemName);
            Assert.Equal("order-flow-pubsub", frameworkOptions.SystemPubSubName);
        }
        finally
        {
            Environment.SetEnvironmentVariable(Constants.SystemNameEnvironmentVariable, null);
        }
    }

    [Fact]
    public void AddIntropyFramework_PrefersTheSystemNameFromEnv_OverTheFunc()
    {
        // The deployment knows which system the component runs in; a name in code must not pin it.
        Environment.SetEnvironmentVariable(Constants.SystemNameEnvironmentVariable, "from-env");
        try
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddIntropyFramework(opts =>
            {
                opts.ComponentName = "orders";
                opts.ServiceNamespace = "example";
                opts.SystemName = "from-func";
            });

            var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

            Assert.Equal("from-env", frameworkOptions.SystemName);
        }
        finally
        {
            Environment.SetEnvironmentVariable(Constants.SystemNameEnvironmentVariable, null);
        }
    }

    [Fact]
    public void AddIntropyFramework_FallsBackToTheSystemNameTheFuncSets_WhenEnvIsUnset()
    {
        Environment.SetEnvironmentVariable(Constants.SystemNameEnvironmentVariable, null);
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddIntropyFramework(opts =>
        {
            opts.ComponentName = "orders";
            opts.ServiceNamespace = "example";
            opts.SystemName = "from-func";
        });

        var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

        Assert.Equal("from-func", frameworkOptions.SystemName);
    }

    [Fact]
    public void AddIntropyFramework_LeavesTheSystemUnset_WhenNeitherSetsIt()
    {
        Environment.SetEnvironmentVariable(Constants.SystemNameEnvironmentVariable, null);
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddIntropyFramework(opts =>
        {
            opts.ComponentName = "orders";
            opts.ServiceNamespace = "example";
        });

        var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

        Assert.Null(frameworkOptions.SystemName);
        Assert.Null(frameworkOptions.SystemPubSubName);
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
