# Intropy.Framework.Blocks

Reusable pipeline blocks for the [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework) pipeline framework.

Each block is a ready-made pipeline shape for a common integration pattern:

| Block | Input → Output | Use case |
|-------|---------------|----------|
| **Extractor** | `string` → `CloudEvent` | System-to-queue: extract data, transform, publish as CloudEvent |
| **Loader** | `CloudEvent` → `TOutput` | Queue-to-system: consume CloudEvent, transform, send |
| **TransactionalIntegration** | File-based | Receive files → enqueue → process → send, with idempotency |

Each block exposes a fluent builder and wires up deserialization, validation, transformation, idempotency, and finalizers in the right order.

## Install

```bash
dotnet add package Intropy.Framework.Blocks
```

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
