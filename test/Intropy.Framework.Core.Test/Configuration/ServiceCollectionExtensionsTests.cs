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
    public void AddIntropyFramework_ReadsThePubSubNameFromEnv_AlongsideTheFunc()
    {
        // Code sets the component's own name; the pub/sub it runs on is the deployment's to name.
        Environment.SetEnvironmentVariable(Constants.PubSubNameEnvironmentVariable, "order-flow-queue");
        try
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddIntropyFramework(opts =>
            {
                opts.ComponentName = "orders";
                opts.ServiceNamespace = "example";
            });

            var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

            Assert.Equal("order-flow-queue", frameworkOptions.PubSubName);
        }
        finally
        {
            Environment.SetEnvironmentVariable(Constants.PubSubNameEnvironmentVariable, null);
        }
    }

    [Fact]
    public void AddIntropyFramework_PrefersThePubSubNameFromEnv_OverTheFunc()
    {
        // The deployment names the components it renders; a name in code must not pin it.
        Environment.SetEnvironmentVariable(Constants.PubSubNameEnvironmentVariable, "from-env");
        try
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddIntropyFramework(opts =>
            {
                opts.ComponentName = "orders";
                opts.ServiceNamespace = "example";
                opts.PubSubName = "from-func";
            });

            var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

            Assert.Equal("from-env", frameworkOptions.PubSubName);
        }
        finally
        {
            Environment.SetEnvironmentVariable(Constants.PubSubNameEnvironmentVariable, null);
        }
    }

    [Fact]
    public void AddIntropyFramework_FallsBackToThePubSubNameTheFuncSets_WhenEnvIsUnset()
    {
        Environment.SetEnvironmentVariable(Constants.PubSubNameEnvironmentVariable, null);
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddIntropyFramework(opts =>
        {
            opts.ComponentName = "orders";
            opts.ServiceNamespace = "example";
            opts.PubSubName = "from-func";
        });

        var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

        Assert.Equal("from-func", frameworkOptions.PubSubName);
    }

    [Fact]
    public void AddIntropyFramework_LeavesThePubSubNameUnset_WhenNeitherSetsIt()
    {
        Environment.SetEnvironmentVariable(Constants.PubSubNameEnvironmentVariable, null);
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddIntropyFramework(opts =>
        {
            opts.ComponentName = "orders";
            opts.ServiceNamespace = "example";
        });

        var frameworkOptions = serviceCollection.BuildServiceProvider().GetRequiredService<FrameworkOptions>();

        Assert.Null(frameworkOptions.PubSubName);
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
