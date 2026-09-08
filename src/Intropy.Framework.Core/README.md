# Intropy.Framework.Core

Core of the Intropy.Framework: a type-safe, observable pipeline framework for building system integrations in .NET.

This package provides the pipeline engine, step abstractions, and result types. Use it when you want to compose business and technical logic into a pipeline with explicit success/failure semantics and built-in OpenTelemetry tracing.

## Install

```bash
dotnet add package Intropy.Framework.Core
```

## Concepts

- **Steps** — `Step<TIn, TOut, TCtx>`, `BusinessStep<...>`, `TechnicalStep<...>`, and `Finalizer<...>` express different failure semantics.
- **Results** — `StepResult<T>` is one of `Success`, `Cancelled`, `BusinessFailure`, `TechnicalFailure`, or `Aborted`. Failures short-circuit subsequent steps; finalizers run conditionally based on `FinalizerTrigger` flags.
- **Context** — Core accepts any `TCtx`; the shared `Context` record belongs to Blocks. Core chains carry `(Result, Context, CancellationToken)`, while overrides and tracing return pairs.

## Companion packages

- `Intropy.Framework.Blocks` — pipeline blocks (Extractor, Loader, TransactionalIntegration)
- `Intropy.Framework.Adapters` — file adapters (SFTP, local, Azure Blob)
- `Intropy.Framework.Hosting` — runtime orchestration with Dapr
- `Intropy.Framework.EventDispatcher` — CloudEvent routing to typed handlers

## Documentation

- [Pipeline and configuration](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/core/pipeline.md)
- [Step contracts](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/core/steps.md)
- [Results](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/core/results.md)

Links point to development-branch docs. For a released package, select its corresponding tag/commit in GitHub before following examples.

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
