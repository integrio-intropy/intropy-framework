using System.Reflection;
using Intropy.Framework.EventDispatcher.Abstractions;

namespace Intropy.Framework.EventDispatcher.Dispatcher;

/// <summary>
/// Manages discovery and registration of CloudEvent handlers from assemblies.
/// </summary>
public class HandlerRegistry
{
    private readonly Dictionary<string, HandlerDescriptor> _handlers = new();

    /// <summary>
    /// Scans the specified assembly for classes decorated with <see cref="CloudEventTypeAttribute"/>
    /// that implement <see cref="ICloudEventHandler{TData}"/> and registers them.
    /// </summary>
    /// <param name="assembly">The assembly to scan for handlers.</param>
    /// <exception cref="InvalidOperationException">Thrown when duplicate event type registrations are detected.</exception>
    public void RegisterHandlersFromAssembly(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        
        var handlerTypes = assembly.GetTypes()
            .Where(t => t is { IsAbstract: false, IsInterface: false })
            .Where(t => t.GetCustomAttribute<CloudEventTypeAttribute>() is not null)
            .Where(t => t.GetInterfaces().Any(IsCloudEventHandler));

        foreach (var handlerType in handlerTypes)
        {
            Register(handlerType);
        }
    }

    internal HandlerDescriptor? GetHandler(string eventType)
    {
        return _handlers.GetValueOrDefault(eventType);
    }

    internal IReadOnlyCollection<HandlerDescriptor> GetAllHandlers() => _handlers.Values;

    private void Register(Type handlerType)
    {
        var attribute = handlerType.GetCustomAttribute<CloudEventTypeAttribute>()
                        ?? throw new ArgumentException(
                            $"Handler {handlerType.Name} is missing [CloudEventType] attribute");

        var handlerInterface = handlerType.GetInterfaces().FirstOrDefault(IsCloudEventHandler)
                               ?? throw new ArgumentException(
                                   $"Handler {handlerType.Name} does not implement ICloudEventHandler<TData>");

        var dataType = handlerInterface.GetGenericArguments()[0];

        if (_handlers.ContainsKey(attribute.EventType))
        {
            throw new InvalidOperationException(
                $"Duplicate handler registration for event type '{attribute.EventType}'");
        }

        _handlers[attribute.EventType] = new HandlerDescriptor
        {
            EventType = attribute.EventType, HandlerType = handlerType, DataType = dataType
        };
    }

    private static bool IsCloudEventHandler(Type type)
    {
        return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ICloudEventHandler<>);
    }
}
