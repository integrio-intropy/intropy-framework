# Intropy.Framework.Blocks

Reusable pipeline blocks for the [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework) pipeline framework.

Each block is a ready-made pipeline shape for a common integration pattern:

| Block | Input → Output | Use case |
|-------|---------------|----------|
| **Extractor** | `string` → `CloudEvent` | System-to-queue: extract data, transform, publish as CloudEvent |
| **Loader** | `CloudEvent` → `TOutput` | Queue-to-system: consume CloudEvent, transform, send |
| **TransactionalIntegration** | File-based | Receive → enqueue → complete; process messages with optional idempotency |

Each block has a fluent builder and fixed execution order. Extractor/Loader builders require idempotency and incident routing; TI makes these optional. Blocks does not supply a subscriber: use Hosting for a TI job, or your own caller for Extractor/Loader.

## Install

```bash
dotnet add package Intropy.Framework.Blocks
```

## Documentation

- [Builders](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/core/builders.md)
- [Extractor](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/blocks/extractor.md)
- [Loader](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/blocks/loader.md)
- [Transactional Integration](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/blocks/transactional-integration.md)

Links point to development-branch docs. For a released package, select its corresponding tag/commit in GitHub before following examples.

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
