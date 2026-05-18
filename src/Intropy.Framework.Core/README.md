# Intropy.Framework.Core

Core of the Intropy.Framework: a type-safe, observable pipeline framework for building system integrations in .NET.

This package provides the pipeline engine, step abstractions, and result types. Use it when you want to compose business and technical logic into a pipeline with explicit success/failure semantics and built-in OpenTelemetry tracing.

## Install

```bash
dotnet add package Intropy.Framework.Core
```

## Concepts

- **Steps** — `Step<TIn, TOut, TCtx>`, `BusinessStep<...>`, `TechnicalStep<...>`, and `Finalizer<...>` express different failure semantics.
- **Results** — `StepResult<T>` is one of `Success`, `Cancelled`, `BusinessFailure`, or `TechnicalFailure`. Failures short-circuit subsequent steps; finalizers run conditionally based on `FinalizerTrigger` flags.
- **Context** — a typed `Context` flows through every step in the pipeline.

## Companion packages

- `Intropy.Framework.Blocks` — pipeline blocks (Extractor, Loader, TransactionalIntegration)
- `Intropy.Framework.Adapters` — file adapters (SFTP, local, Azure Blob)
- `Intropy.Framework.Hosting` — runtime orchestration with Dapr
- `Intropy.Framework.EventDispatcher` — CloudEvent routing to typed handlers

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
