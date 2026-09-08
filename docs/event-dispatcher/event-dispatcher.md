# Event Dispatcher

> Type-safe CloudEvent routing to handlers based on event type attributes.

**Assembly:** `Intropy.Framework.EventDispatcher`

This reference describes the source at this documentation revision. Match it to the tag or commit for your installed package.

## Overview

Decorate a concrete handler with `[CloudEventType("...")]` and implement `ICloudEventHandler<TData>`. `AddCloudEventHandlers` scans the supplied assemblies during registration, builds a singleton registry, and registers the dispatcher and handlers as **scoped** services.

At runtime `CloudEventDispatcher.DispatchAsync` selects one handler by the event's type, converts the data, and invokes the public `HandleAsync` method. Duplicate event-type registrations throw during scanning. Register a public handler method rather than an explicit interface implementation, since dispatch uses reflection by method name.

```mermaid
flowchart LR
    CE[Decoded CloudEvent] --> D[CloudEventDispatcher]
    D -->|order.created| H[OrderCreatedHandler]
    H --> A[Application logic]
```

The dispatcher is not a transport, broker subscriber, or pipeline wrapper. The application owns CloudEvent decoding, acknowledgement, retry, and exception handling.

## Installation

```bash
dotnet add package Intropy.Framework.EventDispatcher
```

No Dapr sidecar is required for dispatch itself.

## Quick example

Complete `Program.cs` for a .NET 10 console project with the EventDispatcher package and implicit usings enabled:

```csharp
using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Framework.EventDispatcher.Abstractions;
using Intropy.Framework.EventDispatcher.DependencyInjection;
using Intropy.Framework.EventDispatcher.Dispatcher;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
services.AddCloudEventHandlers(typeof(OrderCreatedHandler).Assembly);
using var provider = services.BuildServiceProvider();
using var scope = provider.CreateScope();
var dispatcher = scope.ServiceProvider.GetRequiredService<CloudEventDispatcher>();

var cloudEvent = new CloudEvent
{
    Id = "event-1001",
    Source = new Uri("urn:example:orders"),
    Type = "com.company.order.created",
    DataContentType = "application/json",
    Data = JsonSerializer.SerializeToElement(new OrderData("ORD-1001", 100m))
};
await dispatcher.DispatchAsync(cloudEvent, CancellationToken.None);

public record OrderData(string OrderId, decimal Amount);

[CloudEventType("com.company.order.created")]
public sealed class OrderCreatedHandler : ICloudEventHandler<OrderData>
{
    public Task HandleAsync(OrderData data, CloudEvent cloudEvent, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        Console.WriteLine($"{cloudEvent.Id}: {data.OrderId} ({data.Amount})");
        return Task.CompletedTask;
    }
}
```

## Data and error contracts

**Namespaces:**

- Attributes and handler interface: `Intropy.Framework.EventDispatcher.Abstractions`
- Registration: `Intropy.Framework.EventDispatcher.DependencyInjection`
- Dispatcher and options: `Intropy.Framework.EventDispatcher.Dispatcher`

The dispatcher accepts `CloudEvent.Data` as either a `JsonElement` (deserialized to the handler's type) or an object whose runtime type exactly matches `TData`. It does not parse JSON strings or byte arrays on your behalf. Null data and incompatible types throw `InvalidOperationException`; malformed JSON conversion can throw a `JsonException`.

A null event or null Type throws `ArgumentNullException`. An unknown Type throws `InvalidOperationException`. Exceptions from handlers propagate to the caller; synchronous throws may be reflection-wrapped. No `StepResult` conversion is performed. The cancellation token is forwarded to the handler, which owns observing it.

For HTTP ingestion, first decode the envelope with a CloudEvent-aware formatter/binding supporting the mode you accept. Do not assume generic ASP.NET JSON binding into `CloudEvent` handles structured and binary CloudEvents correctly.

## JSON configuration

Optional registration fragment, used **instead of** the registration in the complete example:

```csharp
services.AddCloudEventHandlers(
    options => options.JsonSerializerOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web),
    typeof(OrderCreatedHandler).Assembly);
```

Without configuration, `JsonSerializerOptions` is null and System.Text.Json defaults apply. Configuration affects `JsonElement` conversion, not already-typed payloads.

## Source and related reference

- [Registration](../../src/Intropy.Framework.EventDispatcher/DependencyInjection/ServiceCollectionExtension.cs)
- [CloudEventDispatcher.cs](../../src/Intropy.Framework.EventDispatcher/Dispatcher/CloudEventDispatcher.cs)
- [HandlerRegistry.cs](../../src/Intropy.Framework.EventDispatcher/Dispatcher/HandlerRegistry.cs)
- [Loader](../blocks/loader.md) — a pipeline your transport or handler can call
