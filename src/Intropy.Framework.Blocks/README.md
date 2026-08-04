# Intropy.Framework.Blocks

Reusable pipeline blocks for the [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework) pipeline framework.

Each block is a ready-made pipeline shape for a common integration pattern:

| Block | Input → Output | Use case |
|-------|---------------|----------|
| **Extractor** | `string` → `CloudEvent` | System-to-queue: extract data, transform, publish as CloudEvent |
| **Loader** | `CloudEvent` → `TOutput` | Queue-to-system: consume CloudEvent, transform, send |
| **TransactionalIntegration** | File-based | Receive files → enqueue → process → send, with idempotency |

Each block exposes a fluent builder and wires up deserialization, validation, transformation, idempotency, and finalizers in the right order.

## Configuring steps: DI vs. instance

The builders follow one rule for how steps are supplied:

- **External edges are DI-resolvable.** Idempotency (`WithIdempotency`), business incidents
  (`WithBusinessIncidents`), and the send/publish edge (`WithDaprTopicPublisher`,
  `WithDaprServiceInvoker`, `WithSenderFromServices`) resolve their infrastructure clients from the
  service provider. This keeps the "which external system?" decision in service registration, and lets
  tests substitute a fake by replacing the registration.
- **Component logic is instance-passed.** Deserializers, validators, transformers, serializers, and
  extract steps are component-owned and passed directly to the builder (`WithDeserializer`,
  `WithTransformer`, ...).

For the send edge, `WithSenderFromServices()` resolves the sender registered against its abstract base
type — `SendStep<TCtx>` for the Extractor, `SendStep<TOutput, TCtx>` for the Loader:

```csharp
// Registration (composition root)
services.AddSingleton<SendStep<OrderContext>>(sp =>
    new DaprTopicPublisher<OrderContext>(
        sp.GetRequiredService<DaprClient>(), "pubsub", "orders",
        new Uri("urn:company:system:erp"), "com.company.order.extracted"));

// Builder
var extractor = ExtractorBuilder<Order, OrderDto, OrderContext>.Create("orders", sp)
    // ... other steps ...
    .WithSenderFromServices()
    .Build();
```

The sender is resolved once at `Build()` time — register it as a singleton (or transient), not scoped.

## Install

```bash
dotnet add package Intropy.Framework.Blocks
```

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
