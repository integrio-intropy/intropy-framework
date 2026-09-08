# Intropy.Framework.EventDispatcher

CloudEvent routing for the [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework) pipeline framework.

Resolves incoming CloudEvents to typed handlers via dependency injection, deserializes the event data, and dispatches to the matching `ICloudEventHandler<TData>` implementation. Handlers are discovered by scanning assemblies for the `[CloudEventType("...")]` attribute.

## Install

```bash
dotnet add package Intropy.Framework.EventDispatcher
```

## Usage

Register handlers with `services.AddCloudEventHandlers(typeof(OrderCreatedHandler).Assembly)` (namespace `Intropy.Framework.EventDispatcher.DependencyInjection`). Implement `ICloudEventHandler<TData>` with a public `HandleAsync(TData data, CloudEvent cloudEvent, CancellationToken ct = default)` method and add `[CloudEventType("...")]`.

The dispatcher and handlers are scoped. Decode the CloudEvent in your transport, then resolve `CloudEventDispatcher` in a scope and call `DispatchAsync`. Data must be a `JsonElement` or an exactly matching typed object, not a JSON string. No Dapr sidecar is needed for dispatch itself.

## Documentation

- [Complete example and API contracts](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/event-dispatcher/event-dispatcher.md)

Links point to development-branch docs. For a released package, select its corresponding tag/commit in GitHub before following examples.

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
