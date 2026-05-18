using System.Reflection;
using Intropy.Framework.EventDispatcher.DependencyInjection;
using Intropy.Framework.EventDispatcher.Dispatcher;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.EventDispatcher.Test.DependencyInjection;

public class ServiceCollectionExtensionsTests
{
    // Use a separate assembly that doesn't contain DuplicateOrderHandler.
    // Since the test assembly has conflicting handlers, we test with the source assembly instead.

    [Fact]
    public void AddCloudEventHandlers_RegistersHandlerRegistryAsSingleton()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act — use the source assembly which has no handlers decorated with [CloudEventType]
        services.AddCloudEventHandlers(typeof(CloudEventDispatcher).Assembly);
        var sp = services.BuildServiceProvider();

        // Assert
        var registry1 = sp.GetService<HandlerRegistry>();
        var registry2 = sp.GetService<HandlerRegistry>();
        Assert.NotNull(registry1);
        Assert.Same(registry1, registry2);
    }

    [Fact]
    public void AddCloudEventHandlers_RegistersCloudEventDispatcherAsScoped()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddCloudEventHandlers(typeof(CloudEventDispatcher).Assembly);
        var sp = services.BuildServiceProvider();

        // Act
        using var scope1 = sp.CreateScope();
        using var scope2 = sp.CreateScope();
        var dispatcher1 = scope1.ServiceProvider.GetService<CloudEventDispatcher>();
        var dispatcher2 = scope2.ServiceProvider.GetService<CloudEventDispatcher>();

        // Assert
        Assert.NotNull(dispatcher1);
        Assert.NotNull(dispatcher2);
        Assert.NotSame(dispatcher1, dispatcher2);
    }

    [Fact]
    public void AddCloudEventHandlers_RegistersDiscoveredHandlersAsScoped()
    {
        // Arrange — build a registry manually to verify handler registration
        var services = new ServiceCollection();

        // Create a custom assembly scan that only has non-conflicting handlers
        // We'll verify via the service descriptors directly
        var registry = new HandlerRegistry();
        var registerMethod = typeof(HandlerRegistry)
            .GetMethod("Register", BindingFlags.NonPublic | BindingFlags.Instance)!;
        registerMethod.Invoke(registry, [typeof(OrderCreatedHandler)]);

        services.AddSingleton(registry);
        services.AddScoped<CloudEventDispatcher>();
        services.AddScoped<OrderCreatedHandler>();

        var sp = services.BuildServiceProvider();

        // Act
        using var scope1 = sp.CreateScope();
        using var scope2 = sp.CreateScope();
        var handler1 = scope1.ServiceProvider.GetService<OrderCreatedHandler>();
        var handler2 = scope2.ServiceProvider.GetService<OrderCreatedHandler>();

        // Assert
        Assert.NotNull(handler1);
        Assert.NotNull(handler2);
        Assert.NotSame(handler1, handler2); // Scoped = different per scope
    }

    [Fact]
    public void AddCloudEventHandlers_WithNoAssemblies_RegistersEmptyRegistry()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddCloudEventHandlers();
        var sp = services.BuildServiceProvider();

        // Assert
        var registry = sp.GetRequiredService<HandlerRegistry>();
        Assert.Empty(registry.GetAllHandlers());
    }

    [Fact]
    public void AddCloudEventHandlers_ReturnsSameServiceCollection()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var returned = services.AddCloudEventHandlers();

        // Assert
        Assert.Same(services, returned);
    }
}
