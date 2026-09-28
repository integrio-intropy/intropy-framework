# Intropy.Framework.Adapters

File adapters for the [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework) pipeline framework, built on [Dapr](https://dapr.io/) bindings.

Supports SFTP, local filesystem, and Azure Blob Storage through a uniform abstraction, so pipeline steps don't have to care which transport they're reading from or writing to.

## Install

```bash
dotnet add package Intropy.Framework.Adapters
```

## Requirements

A Dapr sidecar with the relevant binding component(s) configured (`bindings.sftp`, `bindings.localstorage`, `bindings.azure.blobstorage`, etc.).

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.

## Ports

A component registers each of its port's adapters by role; both read the port's adapter from
`Ports:<port>` and key it by the port name. `Intropy.Framework.Adapters` owns the destination
side; the source side (`AddSourcePort`) lives in `Intropy.Framework.Hosting` — see its README:

```csharp
services.AddDestinationPort("orders-destination", configuration);  // a port your code writes through (any number)
```

To declare the port the framework sweeps (one per component, one per file-driven job), and
choose its `FileCompletion`, register through `Intropy.Framework.Hosting`:

```csharp
using Intropy.Framework.Hosting.FileSweeps;
services.AddSourcePort("orders-source", configuration, FileCompletion.Archive("archive")); // completion optional; default: delete
```

`AddSourcePort` also registers the port's adapter from `Ports:<port>` and declares the port as
the component's `SourcePort`; an extractor or a transactional integration sweeps it, and
fails at startup when no source port is registered. When you register the source's adapter
yourself (a custom adapter, or a test fake), declare the port with `AddSourcePort(port)`.
Both build on `AddFileAdapter`, below.

## Configuration-driven keyed transports

Each adapter is registered under its port name, and the caller supplies one configuration
section per port, conventionally `Ports:<port>` (the section the system host also writes
`RootPath` into; the adapter accepts and ignores it):

```json
{
  "Ports": {
    "orders-source": { "Kind": "Sftp", "BasePath": "/inbound" },
    "orders-destination": { "Kind": "Local", "BasePath": "outbound" }
  }
}
```

```csharp
services.AddFileAdapter("orders-source", configuration.GetSection("Ports:orders-source"));
services.AddFileAdapter("orders-destination", configuration.GetSection("Ports:orders-destination"));
// Alternatively pass a FileTransportOptions instance with an explicit Kind.
var source = provider.GetRequiredKeyedService<IFileAdapter>("orders-source");
```

Import `Intropy.Framework.Adapters.File` and `Microsoft.Extensions.DependencyInjection`.
The port name is the one name for the port everywhere: the adapter's key, its configuration
section, and the Dapr binding it talks to (`DaprBindingName` overrides the binding only when a
binding is named differently). `Kind` selects the binding the port is deployed with — `Local`
(`bindings.localstorage`), `Sftp` (`bindings.sftp`), or `AzureBlob`
(`bindings.azure.blobstorage`). The caller must register `DaprClient`. Registration and
resolution make no Dapr calls. Adapters are lazy keyed singletons; no unkeyed adapter or shared
options are registered.

Registration validates immediately, without requiring Generic Host startup: Kind must
be explicit and defined, an explicit binding name nonblank, settings known and scalar, and regexes
valid. `BasePath` defaults to the binding root (`""`) and is passed unchanged to the
existing adapter. Optional `FileNameRegex` patterns use a one-second match timeout.
Configuration/options are snapshotted per key; changes require restart. Credentials
remain in Dapr components, not these options. Errors report the section/key, not values.
A second framework registration for a key is rejected, even after adapter removal.
Unknown key lookups never fall back to another registration.

Tests or consumers can deliberately replace only one edge using normal keyed DI:

```csharp
services.RemoveAllKeyed<IFileAdapter>("orders-source");
services.AddKeyedSingleton<IFileAdapter>("orders-source", new InMemoryFileAdapter());
```

The removal extension is in `Microsoft.Extensions.DependencyInjection.Extensions`;
`InMemoryFileAdapter` is in `Intropy.Framework.Testing.Adapters`. Destination is unchanged.
No adapter protocol, encoding, exception, or empty-content semantics are changed by
registration. In particular, SFTP absolute paths retain their leading slash.

## Escape hatch: a custom adapter via factory

When the binding kind has no `Kind` value (a custom Dapr binding component, or an adapter
assembled from services of its own), register it through the factory overload. The factory runs
at first resolution, inside the provider; the component owns everything else (options, the
`DaprClient` lifetime):

```csharp
services.AddFileAdapter("orders-source", provider =>
    new CustomsBindingAdapter(provider.GetRequiredService<DaprClient>(),
        new FileAdapterOptions("custom-binding", basePath: "inbound")));
```

It participates in the same rules as the config-driven overloads: the adapter is a lazy keyed
singleton under the key, and a second framework registration for a key is rejected regardless of
which overload registered first. Prefer implementing `IFileAdapter` directly; the shared
Dapr-binding base the framework adapters derive from cannot be derived outside the framework.

## Cancellation

Every `IFileAdapter` method — the two `GetContentAsync` overloads, both `WriteAsync` overloads,
`ListAsync`, and `DeleteAsync` — takes an optional `CancellationToken` last, forwarded into the
Dapr binding call where the transport supports it. Accessors that read on demand forward it too:
`SweptFile.ReadAsync` / `SweptFile.ReadTextAsync` accept a token, and the sweep and the jobs
that run it flow the host's cancellation into listing and reads. One deliberate exception:
`FileCompletion.CompleteAsync` takes no cancellation token — completion of an already handled
file is always awaited to the end, even when the host cancels the sweep.

## Sweeping a source

`FileSweep` and the source-port registration (`AddSourcePort`) live in the
`Intropy.Framework.Hosting` package (`Intropy.Framework.Hosting.FileSweeps` namespace) — see the
[Sweeping a source](../Intropy.Framework.Hosting/README.md#sweeping-a-source) section of its
README. `Intropy.Framework.Adapters` supplies the `IFileAdapter` implementations, the options,
and `AddFileAdapter` / `AddDestinationPort`; extractors and transactional integrations consume
them through `Intropy.Framework.Hosting`.

