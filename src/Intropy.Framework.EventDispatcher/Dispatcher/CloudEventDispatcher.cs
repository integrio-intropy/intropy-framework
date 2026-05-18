using System.Text.Json;
using CloudNative.CloudEvents;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Intropy.Framework.EventDispatcher.Dispatcher;

/// <summary>
/// Dispatches CloudEvents to their registered handlers based on the event type.
/// </summary>
/// <param name="serviceProvider">Service provider used to resolve handler instances.</param>
/// <param name="registry">Registry containing handler registrations.</param>
/// <param name="options">Dispatcher configuration options.</param>
public class CloudEventDispatcher(
    IServiceProvider serviceProvider,
    HandlerRegistry registry,
    IOptions<CloudEventDispatcherOptions> options)
{
    private readonly JsonSerializerOptions? _jsonOptions = options.Value.JsonSerializerOptions;

    /// <summary>
    /// Dispatches a CloudEvent to its registered handler, deserializing the data payload to the expected type.
    /// </summary>
    /// <param name="cloudEvent">The CloudEvent to dispatch.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="ArgumentNullException">Thrown when the CloudEvent type is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when no handler is registered for the event type, or when data deserialization fails.</exception>
    public async Task DispatchAsync(CloudEvent cloudEvent, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(cloudEvent);
        ArgumentNullException.ThrowIfNull(cloudEvent.Type);

        var descriptor = registry.GetHandler(cloudEvent.Type)
                         ?? throw new InvalidOperationException(
                             $"No handler registered for event type '{cloudEvent.Type}'");

        var handler = serviceProvider.GetRequiredService(descriptor.HandlerType);

        var data = DeserializeData(cloudEvent.Data, descriptor.DataType);

        var method = descriptor.HandlerType.GetMethod("HandleAsync")!;
        await (Task)method.Invoke(handler, [data, cloudEvent, ct])!;
    }

    private object DeserializeData(object? data, Type targetType)
    {
        return data switch
        {
            null => throw new InvalidOperationException("CloudEvent data is null"),
            JsonElement json => json.Deserialize(targetType, _jsonOptions)
                                ?? throw new InvalidOperationException(
                                    $"Failed to deserialize data to {targetType.Name}"),
            _ when data.GetType() == targetType => data,
            _ => throw new InvalidOperationException(
                $"Cannot convert data of type {data.GetType().Name} to {targetType.Name}")
        };
    }
}
