# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build the solution
dotnet build Intropy.Framework.slnx

# Run all tests
dotnet test Intropy.Framework.slnx

# Run tests for a single project
dotnet test test/Intropy.Framework.Core.Test/Intropy.Framework.Core.Test.csproj

# Run a single test class
dotnet test Intropy.Framework.slnx --filter "FullyQualifiedName~ClassName"

# Run a specific test
dotnet test Intropy.Framework.slnx --filter "FullyQualifiedName=Namespace.ClassName.TestMethodName"
```

## Architecture Overview

This is a type-safe, observable pipeline framework for building system integrations in .NET 10. The framework enforces separation between business and technical failures while providing built-in OpenTelemetry tracing and idempotency support. Consumer documentation starts at [docs/index.md](docs/index.md); exact override contracts are in [docs/core/steps.md](docs/core/steps.md).

### Packages

| Package | Purpose |
|---------|---------|
| **Intropy.Framework.Core** | Pipeline engine, step abstractions, result types |
| **Intropy.Framework.Adapters** | File adapters (SFTP, local, Azure Blob) via Dapr bindings |
| **Intropy.Framework.Blocks** | Reusable pipeline blocks (Extractor, Loader, TransactionalIntegration) |
| **Intropy.Framework.Hosting** | Runtime orchestration with Dapr sidecar lifecycle, idle timeout, message subscription |
| **Intropy.Framework.EventDispatcher** | CloudEvent routing to typed handlers via DI |

### Key Concepts

**Pipeline Execution Flow**: Steps execute sequentially via extension methods that chain `Task<(StepResult<T>, TCtx, CancellationToken)>`. If a step fails, subsequent steps are skipped and the failure propagates (unless it's a Finalizer).

**Step Base Classes** (`src/Intropy.Framework.Core/Pipeline/Abstractions/Steps/`):
- `Step<TIn, TOut, TCtx>` - Generic step, uncaught exceptions become technical failures
- `BusinessStep<TIn, TOut, TCtx>` - For domain logic, uncaught exceptions become `BusinessIncidentData`
- `TechnicalStep<TIn, TOut, TCtx>` - For infrastructure, uncaught exceptions become `TechnicalFailure`
- `Finalizer<T, TCtx>` - Runs conditionally based on `FinalizerTrigger` flags (OnSuccess, OnCancelled, OnBusinessFailure, OnTechnicalFailure, OnAborted)

**Result Types** (`src/Intropy.Framework.Core/Pipeline/Abstractions/Results/`):
- `StepResult<T>.Success` - Contains the value
- `StepResult<T>.Cancelled` - Used for idempotency (already processed)
- `StepResult<T>.BusinessFailure` - Contains `BusinessIncidentData`
- `StepResult<T>.TechnicalFailure` - Contains `TechnicalFailure`
- `StepResult<T>.Aborted` - Execution abortion, normally cancellation-token signalling

### Pipeline Blocks

Located in `src/Intropy.Framework.Blocks/`:

- **Extractor** (`string` → `CloudEvent`): deserialize → validate → extract(s) → idempotency check → transform → serialize → send → idempotency record → incident finalizer.
- **Loader** (`CloudEvent` → `TOutput`): deserialize → extract(s) → validate → idempotency check → transform → send → idempotency record → optional receipt → incident finalizer. The caller owns message subscription.
- **TransactionalIntegration**: receive → enqueue → complete; send runs deserialize → extract(s) → optional idempotency check → validate → transform → serialize → send → optional idempotency record → optional incident finalizer. Hosting supplies the job lifecycle. Extractor/Loader builders require idempotency and routing; TI builders do not.

Each block exposes a fluent builder (e.g. `ExtractorBuilder<TInput, TOutput, TCtx>.Create(...)`).

### Context

Core chains accept any `TCtx`; Blocks use the `Context` record (or a derived record). The built-in Hosting lifecycle resolves pipelines of exactly `Context`. Its metadata dictionary is mutable and must be isolated per message. Standard context keys for idempotency are defined in `IdempotencyContextKeys`.

### Configuration and execution caveats

- `AddIntropyFramework` requires both `ComponentName` and `ServiceNamespace`; without a delegate it reads `INTROPY_COMPONENT_NAME` and `INTROPY_SERVICE_NAMESPACE`.
- Step/finalizer overrides accept a cancellation token and return a two-element result/context tuple. Core chains carry a third token element; block `Execute` and tracing return pairs.
- Loader deserialization uses protected `DeserializeAsync` without a token; TI Enqueue uses the special four-parameter override. See the [block matrix](docs/concepts/step-types.md).
- Finalizers replace results; they do not preserve the first business failure. A signalled token prevents even an `OnAborted` finalizer body from running.
- Classification does not imply broker policy. TI requests retry for final technical or unhandled business failures, but currently acknowledges `Aborted`. See [result semantics](docs/concepts/result-types.md).

### Testing

Tests use xUnit with NSubstitute for mocking. Testcontainers is used for Dapr integration tests. Test files mirror the source structure under `test/`, with one test project per source project.
