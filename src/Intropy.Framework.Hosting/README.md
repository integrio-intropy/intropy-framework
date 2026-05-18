# Intropy.Framework.Hosting

Runtime orchestration for [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework) pipelines.

Handles the integration host lifecycle: waits for the Dapr sidecar, runs the pipeline, subscribes to message topics, applies idle timeouts, and shuts down cleanly.

## Install

```bash
dotnet add package Intropy.Framework.Hosting
```

## Requirements

A Dapr sidecar. Used together with `Intropy.Framework.Blocks` to run TransactionalIntegration and similar long-running pipelines.

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
