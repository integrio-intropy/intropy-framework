using CloudNative.CloudEvents;

namespace Intropy.Framework.EventDispatcher.Abstractions;

/// <summary>
/// Defines a handler for CloudEvents with a strongly-typed data payload.
/// </summary>
/// <typeparam name="TData">The type of the deserialized event data.</typeparam>
public interface ICloudEventHandler<in TData>
{
    /// <summary>
    /// Handles an incoming CloudEvent.
    /// </summary>
    /// <param name="data">The deserialized event data.</param>
    /// <param name="cloudEvent">The original CloudEvent including metadata.</param>
    /// <param name="ct">Cancellation token.</param>
    Task HandleAsync(TData data, CloudEvent cloudEvent, CancellationToken ct = default);
}
