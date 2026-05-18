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
