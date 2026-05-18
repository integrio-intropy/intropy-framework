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

This is a type-safe, observable pipeline framework for building system integrations in .NET 10. The framework enforces separation between business and technical failures while providing built-in OpenTelemetry tracing and idempotency support.

### Packages

| Package | Purpose |
|---------|---------|
| **Intropy.Framework.Core** | Pipeline engine, step abstractions, result types |
| **Intropy.Framework.Adapters** | File adapters (SFTP, local, Azure Blob) via Dapr bindings |
| **Intropy.Framework.Blocks** | Reusable pipeline blocks (Extractor, Loader, TransactionalIntegration) |
| **Intropy.Framework.Hosting** | Runtime orchestration with Dapr sidecar lifecycle, idle timeout, message subscription |
| **Intropy.Framework.EventDispatcher** | CloudEvent routing to typed handlers via DI |

### Key Concepts

**Pipeline Execution Flow**: Steps execute sequentially via extension methods that chain `Task<(StepResult<T>, TCtx)>`. If a step fails, subsequent steps are skipped and the failure propagates (unless it's a Finalizer).

**Step Base Classes** (`src/Intropy.Framework.Core/Pipeline/Abstractions/Steps/`):
- `Step<TIn, TOut, TCtx>` - Generic step, uncaught exceptions become technical failures
- `BusinessStep<TIn, TOut, TCtx>` - For domain logic, uncaught exceptions become `BusinessIncident`
- `TechnicalStep<TIn, TOut, TCtx>` - For infrastructure, uncaught exceptions become `TechnicalFailure`
- `Finalizer<T, TCtx>` - Runs conditionally based on `FinalizerTrigger` flags (OnSuccess, OnCancelled, OnBusinessFailure, OnTechnicalFailure)

**Result Types** (`src/Intropy.Framework.Core/Pipeline/Abstractions/Results/`):
- `StepResult<T>.Success` - Contains the value
- `StepResult<T>.Cancelled` - Used for idempotency (already processed)
- `StepResult<T>.BusinessFailure` - Contains `BusinessIncident`
- `StepResult<T>.TechnicalFailure` - Contains `TechnicalFailure`

### Pipeline Blocks

Located in `src/Intropy.Framework.Blocks/`:

- **Extractor** (`string` → `CloudEvent`): system-to-queue. Extract → deserialize → idempotency → validate → transform → serialize → publish (Dapr pub/sub or service invoke).
- **Loader** (`CloudEvent` → `TOutput`): queue-to-system. Consume CloudEvent → transform → send.
- **TransactionalIntegration**: file-based full lifecycle. Receive files → enqueue → process → send, with idempotency.

Each block exposes a fluent builder (e.g. `ExtractorBuilder<TInput, TOutput, TCtx>.Create(...)`).

### Context

Pipelines use a `Context` class (or subclass) that flows through all steps. Standard context keys for idempotency are defined in `IdempotencyContextKeys`.

### Testing

Tests use xUnit with NSubstitute for mocking. Testcontainers is used for Dapr integration tests. Test files mirror the source structure under `test/`, with one test project per source project.
