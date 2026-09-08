# Intropy.Framework

A type-safe, observable pipeline framework for building system integrations in .NET.

Every pipeline is a chain of typed steps — deserialize, validate, transform, serialize, send — with built-in OpenTelemetry tracing and a result type system that enforces separation between business and technical failures.

## Why?

Integrations between systems share the same problems: data needs to be moved, transformed, validated, and the failure modes need to be classified. *This* failure is a bad payload (business problem — route to a person), *that* failure is a flaky network call (technical problem — retry). Idempotency, observability, and orderly failure handling all need to be wired up consistently.

Intropy.Framework packages the structure: declare your steps, compose them with a builder, and the framework handles execution, tracing, idempotency hooks, and result classification.

## Packages

| Package | Description |
|---------|-------------|
| [Intropy.Framework.Core](https://www.nuget.org/packages/Intropy.Framework.Core) | Pipeline engine, step abstractions, result types |
| [Intropy.Framework.Blocks](https://www.nuget.org/packages/Intropy.Framework.Blocks) | Reusable pipeline blocks (Extractor, Loader, Transactional Integration) |
| [Intropy.Framework.Adapters](https://www.nuget.org/packages/Intropy.Framework.Adapters) | File adapters (SFTP, local, Azure Blob) via Dapr bindings |
| [Intropy.Framework.Hosting](https://www.nuget.org/packages/Intropy.Framework.Hosting) | Runtime orchestration (Dapr sidecar lifecycle, message subscription, idle timeout) |
| [Intropy.Framework.EventDispatcher](https://www.nuget.org/packages/Intropy.Framework.EventDispatcher) | Attribute-based CloudEvent routing to typed handlers |

For a full Transactional Integration job, install **Hosting** — it transitively includes Blocks, Adapters, and Core.

## Quick start

```bash
dotnet add package Intropy.Framework.Hosting
```

Composition fragment, using the application step classes and service registrations from the walkthrough:

```csharp
builder.Services.AddIntropyFramework(opts =>
{
    opts.ComponentName = "order-processor";
    opts.ServiceNamespace = "example";
});

builder.Services.AddSendPipeline<Order, Invoice, Context>("order-send", (pb, sp) => pb
    .WithDeserializer(new OrderDeserializer())
    .WithValidator(new OrderValidator())
    .WithTransformer(new OrderToInvoiceTransformer())
    .WithSerializer(new InvoiceSerializer())
    .WithSender(sp.GetRequiredService<InvoiceApiSender>()));
```

TI idempotency and incident routing are opt-in; omitting them does not provide deduplication or automatic incident creation.

See the [Getting Started guide](docs/getting-started.md) for the full walkthrough.

## Requirements

- .NET 10 or later
- Dapr sidecar for Hosting, built-in file adapters, and Dapr-backed steps; Core and EventDispatcher can run without it

## Documentation

Full documentation lives in [`docs/`](docs/index.md). It describes the source at the same revision; use the tag/commit matching your installed package.

Start here:

- [Implementing pipeline steps](docs/implementing-pipeline-steps.md) — context, result cases, block-specific step signatures, and file writes
- [Getting Started](docs/getting-started.md) — build a complete Transactional Integration from scratch
- [Concepts](docs/concepts/pipeline-execution.md) — pipeline execution, result types, step types, observability
- [Core](docs/core/pipeline.md) — pipeline engine, builders, step types, and result types
- [Blocks](docs/blocks/transactional-integration.md) — Transactional Integration, Extractor, Loader
- [Adapters](docs/adapters/file-adapters.md) — File adapters via Dapr bindings
- [Event Dispatcher](docs/event-dispatcher/event-dispatcher.md) — CloudEvent routing

## Build and test

```bash
dotnet build Intropy.Framework.slnx
dotnet test Intropy.Framework.slnx
```

## License

[MIT](LICENSE) © Integrio
