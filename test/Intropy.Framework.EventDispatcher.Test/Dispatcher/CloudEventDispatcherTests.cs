using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Framework.EventDispatcher.Dispatcher;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Intropy.Framework.EventDispatcher.Test.Dispatcher;

public class CloudEventDispatcherTests
{
    private readonly HandlerRegistry _registry = new();

    private CloudEventDispatcher CreateDispatcher(
        IServiceProvider serviceProvider,
        CloudEventDispatcherOptions? options = null)
    {
        return new CloudEventDispatcher(
            serviceProvider,
            _registry,
            Options.Create(options ?? new CloudEventDispatcherOptions()));
    }

    private static CloudEvent CreateCloudEvent(string type, object? data = null)
    {
        return new CloudEvent
        {
            Type = type, Source = new Uri("https://test.example.com"), Id = Guid.NewGuid().ToString(), Data = data
        };
    }

    private void RegisterHandler<THandler>()
    {
        var method = typeof(HandlerRegistry)
            .GetMethod("Register", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        method.Invoke(_registry, [typeof(THandler)]);
    }

    [Fact]
    public async Task DispatchAsync_WithMatchingTypeData_CallsHandler()
    {
        // Arrange
        RegisterHandler<OrderCreatedHandler>();

        var handler = new OrderCreatedHandler();
        var services = new ServiceCollection();
        services.AddSingleton(handler);
        var sp = services.BuildServiceProvider();

        var dispatcher = CreateDispatcher(sp);
        var data = new TestOrderData("ORD-1", 99.99m);
        var cloudEvent = CreateCloudEvent("order.created", data);

        // Act
        await dispatcher.DispatchAsync(cloudEvent);

        // Assert
        Assert.True(handler.WasCalled);
        Assert.Equal("ORD-1", handler.ReceivedData!.OrderId);
        Assert.Equal(99.99m, handler.ReceivedData.Amount);
        Assert.Same(cloudEvent, handler.ReceivedEvent);
    }

    [Fact]
    public async Task DispatchAsync_WithJsonElementData_DeserializesAndCallsHandler()
    {
        // Arrange
        RegisterHandler<OrderCreatedHandler>();

        var handler = new OrderCreatedHandler();
        var services = new ServiceCollection();
        services.AddSingleton(handler);
        var sp = services.BuildServiceProvider();

        var dispatcher = CreateDispatcher(sp);
        var json = JsonSerializer.SerializeToElement(new TestOrderData("ORD-2", 50.00m));
        var cloudEvent = CreateCloudEvent("order.created", json);

        // Act
        await dispatcher.DispatchAsync(cloudEvent);

        // Assert
        Assert.True(handler.WasCalled);
        Assert.Equal("ORD-2", handler.ReceivedData!.OrderId);
        Assert.Equal(50.00m, handler.ReceivedData.Amount);
    }

    [Fact]
    public async Task DispatchAsync_WithNullEventType_ThrowsArgumentNullException()
    {
        // Arrange
        var sp = new ServiceCollection().BuildServiceProvider();
        var dispatcher = CreateDispatcher(sp);
        var cloudEvent = new CloudEvent
        {
            Source = new Uri("https://test.example.com"),
            // Type is intentionally not set (null)
        };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => dispatcher.DispatchAsync(cloudEvent));
    }

    [Fact]
    public async Task DispatchAsync_WithUnregisteredEventType_ThrowsInvalidOperationException()
    {
        // Arrange
        var sp = new ServiceCollection().BuildServiceProvider();
        var dispatcher = CreateDispatcher(sp);
        var cloudEvent = CreateCloudEvent("unknown.event", new TestOrderData("X", 0));

        // Act & Assert
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => dispatcher.DispatchAsync(cloudEvent));
        Assert.Contains("unknown.event", ex.Message);
    }

    [Fact]
    public async Task DispatchAsync_WithNullData_ThrowsInvalidOperationException()
    {
        // Arrange
        RegisterHandler<OrderCreatedHandler>();

        var handler = new OrderCreatedHandler();
        var services = new ServiceCollection();
        services.AddSingleton(handler);
        var sp = services.BuildServiceProvider();

        var dispatcher = CreateDispatcher(sp);
        var cloudEvent = CreateCloudEvent("order.created", null);

        // Act & Assert
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => dispatcher.DispatchAsync(cloudEvent));
        Assert.Contains("null", ex.Message);
    }

    [Fact]
    public async Task DispatchAsync_WithIncompatibleDataType_ThrowsInvalidOperationException()
    {
        // Arrange
        RegisterHandler<OrderCreatedHandler>();

        var handler = new OrderCreatedHandler();
        var services = new ServiceCollection();
        services.AddSingleton(handler);
        var sp = services.BuildServiceProvider();

        var dispatcher = CreateDispatcher(sp);
        var cloudEvent = CreateCloudEvent("order.created", "just a plain string");

        // Act & Assert
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => dispatcher.DispatchAsync(cloudEvent));
        Assert.Contains("String", ex.Message);
        Assert.Contains("TestOrderData", ex.Message);
    }

    [Fact]
    public async Task DispatchAsync_PassesCancellationToken()
    {
        // Arrange
        RegisterHandler<OrderCreatedHandler>();

        var handler = new OrderCreatedHandler();
        var services = new ServiceCollection();
        services.AddSingleton(handler);
        var sp = services.BuildServiceProvider();

        var dispatcher = CreateDispatcher(sp);
        var data = new TestOrderData("ORD-3", 10m);
        var cloudEvent = CreateCloudEvent("order.created", data);
        using var cts = new CancellationTokenSource();
        var token = cts.Token;

        // Act
        await dispatcher.DispatchAsync(cloudEvent, token);

        // Assert
        Assert.Equal(token, handler.ReceivedCt);
    }

    [Fact]
    public async Task DispatchAsync_WhenHandlerThrowsAsync_PropagatesException()
    {
        // Arrange
        RegisterHandler<AsyncThrowingHandler>();

        var services = new ServiceCollection();
        services.AddSingleton<AsyncThrowingHandler>();
        var sp = services.BuildServiceProvider();

        var dispatcher = CreateDispatcher(sp);
        var data = new TestOrderData("ORD-4", 0m);
        var cloudEvent = CreateCloudEvent("throws.async", data);

        // Act & Assert
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => dispatcher.DispatchAsync(cloudEvent));
        Assert.Equal("Async handler failure", ex.Message);
    }

    [Fact]
    public async Task DispatchAsync_WhenHandlerThrowsSync_PropagatesException()
    {
        // Arrange
        RegisterHandler<SyncThrowingHandler>();

        var services = new ServiceCollection();
        services.AddSingleton<SyncThrowingHandler>();
        var sp = services.BuildServiceProvider();

        var dispatcher = CreateDispatcher(sp);
        var data = new TestOrderData("ORD-5", 0m);
        var cloudEvent = CreateCloudEvent("throws.sync", data);

        // Act & Assert
        // Note: synchronous exceptions from method.Invoke get wrapped in
        // TargetInvocationException. This test documents the current behavior.
        var ex = await Assert.ThrowsAsync<System.Reflection.TargetInvocationException>(() =>
            dispatcher.DispatchAsync(cloudEvent));
        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal("Sync handler failure", ex.InnerException!.Message);
    }

    [Fact]
    public async Task DispatchAsync_WithCustomJsonOptions_UsesOptionsForDeserialization()
    {
        // Arrange
        RegisterHandler<OrderCreatedHandler>();

        var handler = new OrderCreatedHandler();
        var services = new ServiceCollection();
        services.AddSingleton(handler);
        var sp = services.BuildServiceProvider();

        var jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var dispatcher = CreateDispatcher(sp, new CloudEventDispatcherOptions { JsonSerializerOptions = jsonOptions });

        // JSON with UPPER_CASE keys — only deserializes correctly with case-insensitive option
        var json = JsonSerializer.SerializeToElement(
            new { ORDERID = "ORD-6", AMOUNT = 77.00m });
        var cloudEvent = CreateCloudEvent("order.created", json);

        // Act
        await dispatcher.DispatchAsync(cloudEvent);

        // Assert
        Assert.True(handler.WasCalled);
        Assert.Equal("ORD-6", handler.ReceivedData!.OrderId);
        Assert.Equal(77.00m, handler.ReceivedData.Amount);
    }

    [Fact]
    public async Task DispatchAsync_WithDefaultOptions_UsesDefaultJsonBehavior()
    {
        // Arrange
        RegisterHandler<OrderCreatedHandler>();

        var handler = new OrderCreatedHandler();
        var services = new ServiceCollection();
        services.AddSingleton(handler);
        var sp = services.BuildServiceProvider();

        // No custom options — default STJ is case-sensitive
        var dispatcher = CreateDispatcher(sp);

        // JSON with UPPER_CASE keys — won't bind to PascalCase properties with default options
        var json = JsonSerializer.SerializeToElement(
            new { ORDERID = "ORD-7", AMOUNT = 88.00m });
        var cloudEvent = CreateCloudEvent("order.created", json);

        // Act
        await dispatcher.DispatchAsync(cloudEvent);

        // Assert — properties won't bind, so they get default values
        Assert.True(handler.WasCalled);
        Assert.Null(handler.ReceivedData!.OrderId);
        Assert.Equal(0m, handler.ReceivedData.Amount);
    }
}
