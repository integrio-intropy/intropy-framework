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

For a full Transactional Integration worker, install **Blocks** and **Hosting**.

## Quick start

```bash
dotnet add package Intropy.Framework.Blocks
dotnet add package Intropy.Framework.Hosting
```

```csharp
builder.Services.AddIntropyFramework(opts => opts.ComponentName = "order-processor");

builder.Services.AddTransactionalIntegration(opts =>
{
    opts.DaprPubSubName = "pubsub";
    opts.DaprTopicName = "orders";
});

builder.Services.AddSendPipeline<Order, Invoice, Context>("order-to-invoice",
    (pb, sp) => pb
        .WithDeserializer(new OrderDeserializer())
        .WithValidator(new OrderValidator())
        .WithIdempotency(
            sp.GetRequiredService<IIdempotencyServiceClient>(),
            order => order.OrderId,
            order => order.CreatedAt)
        .WithTransformer(new OrderToInvoiceTransformer())
        .WithSerializer(new InvoiceSerializer())
        .WithSender(sp.GetRequiredService<InvoiceApiSender>())
        .WithBusinessIncidents(
            sp.GetRequiredService<IBusinessIncidentServiceClient>(),
            ctx => ctx.Metadata["message_id"]));

var runner = app.Services.GetRequiredService<TransactionalIntegrationRunner>();
return await runner.RunAsync();
```

See the [Getting Started guide](docs/getting-started.md) for the full walkthrough.

## Requirements

- .NET 10 or later
- Dapr sidecar (for the Blocks, Adapters, and Hosting packages)

## Documentation

Full documentation lives in [`docs/`](docs/index.md):

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
