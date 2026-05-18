# Intropy.Framework.EventDispatcher

CloudEvent routing for the [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework) pipeline framework.

Resolves incoming CloudEvents to typed handlers via dependency injection, deserializes the event data, and dispatches to the matching `ICloudEventHandler<TData>` implementation. Handlers are discovered by scanning assemblies for the `[CloudEventType("...")]` attribute.

## Install

```bash
dotnet add package Intropy.Framework.EventDispatcher
```

## Usage

```csharp
services.AddCloudEventDispatcher(options =>
    options.AddHandlersFromAssembly(typeof(Program).Assembly));
```

```csharp
[CloudEventType("com.example.order.created")]
public sealed class OrderCreatedHandler : ICloudEventHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated data, CancellationToken ct) => /* ... */;
}
```

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
