# Intropy.Framework.Adapters

File adapters for the [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework) pipeline framework, built on [Dapr](https://dapr.io/) bindings.

Supports SFTP, local filesystem, and Azure Blob Storage through a uniform abstraction, so pipeline steps don't have to care which transport they're reading from or writing to.

## Install

```bash
dotnet add package Intropy.Framework.Adapters
```

## Requirements

A Dapr sidecar with the relevant binding component(s) configured (`bindings.sftp`, `bindings.localstorage`, `bindings.azure.blobstorage`, etc.).

## Documentation

- [File adapter reference](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/adapters/file-adapters.md)
- [Implementing pipeline steps](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/implementing-pipeline-steps.md)

Links point to development-branch docs. For a released package, select its corresponding tag/commit in GitHub before following examples.

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
