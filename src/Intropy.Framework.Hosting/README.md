# Intropy.Framework.Hosting

Runtime orchestration for [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework) pipelines.

Handles the integration host lifecycle: waits for the Dapr sidecar, runs the pipeline, subscribes to message topics, applies idle timeouts, and shuts down cleanly.

## Install

```bash
dotnet add package Intropy.Framework.Hosting
```

## Requirements

A dedicated Dapr sidecar with streaming pub/sub, a source lister, receive/send pipelines using `Context`, and registered Dapr clients. Hosting transitively includes Blocks, Adapters, and Core. It implements the TI job lifecycle, not a general-purpose host for every block.

Set both framework identity fields (`ComponentName` and `ServiceNamespace`). The runner attempts sidecar shutdown after lifecycle execution; exit code zero alone does not guarantee delivery of every item.

## Documentation

- [Complete TI walkthrough](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/getting-started.md)
- [Lifecycle and options](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/blocks/transactional-integration.md)

Links point to development-branch docs. For a released package, select its corresponding tag/commit in GitHub before following examples.

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
