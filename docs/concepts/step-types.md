# Step Types

> A step's base class fixes its result family and the failure domain of ordinary uncaught exceptions.

## Choosing a core step type

| Base class | Return family | Ordinary uncaught exception becomes |
|---|---|---|
| `BusinessStep<TIn, TOut, TCtx>` | `BusinessStepResult<TOut>` | `Failure(BusinessIncidentData)` |
| `TechnicalStep<TIn, TOut, TCtx>` | `TechnicalStepResult<TOut>` | `Failure(TechnicalFailure)` |
| `Step<TIn, TOut, TCtx>` | `StepResult<TOut>` | `TechnicalFailure` |
| `Finalizer<T, TCtx>` | `StepResult<T>` | `TechnicalFailure` |

Use a business step for logic whose failures should enter business-incident handling. Use a technical step for logic whose failures should propagate as technical problems to the caller/runtime. Use the generic `Step` when a single step must explicitly return either failure domain.

A finalizer receives the full current result and runs according to its trigger flags; ordinary steps receive only a successful value. Add finalizers with `.AddFinalizer()`, not `.AddStep()`. Finalizers can replace the result, including converting a handled business failure to success.

Exception conversion and tracing are provided by the pipeline wrapper, not by a direct call to your override. Cancellation-token signalling is handled separately as `Aborted`. See [Steps](../core/steps.md) for the exact override and cancellation contracts.

## Block step abstractions

The Blocks package provides named abstractions with fixed `StepName` values and `TCtx : Context`. Derive from the abstraction for **your block**, then return its prescribed result family. Do not choose a family from the name `Send`, `Deserialize`, or `Serialize` alone.

In the tables below, **business** means `BusinessStepResult<TOut>` and **technical** means `TechnicalStepResult<TOut>`. Every table uses types in the stated namespace; `Context` lives in `Intropy.Framework.Blocks.Shared`.

### Transactional Integration — send pipeline

**Namespace:** `Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps`

[Source](../../src/Intropy.Framework.Blocks/TransactionalIntegration/Send/Steps/)

| Abstract class | Domain | Input → Output |
|---|---|---|
| `DeserializeStep<TInput, TCtx>` | Business | `ReadOnlyMemory<byte>` → `TInput` |
| `ExtractStep<TInput, TCtx>` | Business | `TInput` → `TInput` |
| `ValidateStep<T, TCtx>` | Business | `T` → `T` |
| `TransformStep<TInput, TOutput, TCtx>` | Technical | `TInput` → `TOutput` |
| `SerializeStep<T, TCtx>` | Technical | `T` → `string` |
| `SendStep<TCtx>` | **Business** | `string` → `string` |

These steps use `public override Task<(ResultFamily<TOut> Result, TCtx Context)> ExecuteAsync(TIn input, TCtx context, CancellationToken ct)`, substituting the result family and types from the table.

### Transactional Integration — receive pipeline

**Namespace:** `Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps`

[Source](../../src/Intropy.Framework.Blocks/TransactionalIntegration/Receive/Steps/)

| Abstract class | Domain | Input → Output |
|---|---|---|
| `ReceiveStep<TCtx>` | Business | `SourceItemInfo` → `SourceItem` |
| `EnqueueStep<TCtx>` | Technical | `SourceItem` → `SourceItem` |
| `CompleteStep<TCtx>` | Business | `SourceItem` → `SourceItem` |

`SourceItemInfo` and `SourceItem` live in `Intropy.Framework.Blocks.TransactionalIntegration.Receive`. Receive and Complete use the ordinary three-parameter override.

**Enqueue is an exception:** its constructor takes `FrameworkOptions` (`Intropy.Framework.Core.Configuration`). Its three-parameter `ExecuteAsync` is sealed and builds a CloudEvent from the source item and context. Implement this four-parameter overload instead:

```csharp
public abstract Task<(TechnicalStepResult<SourceItem> Result, TCtx Context)> ExecuteAsync(
    SourceItem input, ReadOnlyMemory<byte> cloudEvent, TCtx context, CancellationToken ct);
```

The `cloudEvent` argument is the encoded structured-mode CloudEvent to publish, not just the source payload.

### Extractor

**Namespace:** `Intropy.Framework.Blocks.Extractor.Steps`

[Source](../../src/Intropy.Framework.Blocks/Extractor/Steps/)

| Abstract class | Domain | Input → Output |
|---|---|---|
| `DeserializeStep<TInput, TCtx>` | Business | `string` → `TInput` |
| `ExtractStep<TInput, TCtx>` | Business | `TInput` → `TInput` |
| `ValidateStep<T, TCtx>` | Business | `T` → `T` |
| `TransformStep<TInput, TOutput, TCtx>` | Technical | `TInput` → `TOutput` |
| `SerializeStep<T, TCtx>` | Technical | `T` → `CloudEvent` |
| `SendStep<TCtx>` | **Technical** | `CloudEvent` → `CloudEvent` |

These steps use the ordinary three-parameter `ExecuteAsync` override. `CloudEvent` lives in `CloudNative.CloudEvents`. The built-in send implementations publish to Dapr pub/sub or invoke a Dapr service.

### Loader

**Namespace:** `Intropy.Framework.Blocks.Loader.Steps`

[Source](../../src/Intropy.Framework.Blocks/Loader/Steps/)

| Abstract class | Domain | Input → Output |
|---|---|---|
| `DeserializeStep<TInput, TCtx>` | Business | `CloudEvent` → `TInput` |
| `ExtractStep<TInput, TCtx>` | Business | `TInput` → `TInput` |
| `ValidateStep<T, TCtx>` | Business | `T` → `T` |
| `TransformStep<TInput, TOutput, TCtx>` | Technical | `TInput` → `TOutput` |
| `SendStep<T, TCtx>` | **Technical** | `T` → `T` |

There is no separate Loader `SerializeStep` in this source revision.

**Loader deserialization is another exception:** the three-parameter `ExecuteAsync` is sealed. It copies CloudEvent metadata into `Context.Metadata` before calling this protected override, which has **no cancellation-token parameter**:

```csharp
protected abstract Task<(BusinessStepResult<TInput> Result, TCtx Context)> DeserializeAsync(
    CloudEvent cloudEvent, TCtx context);
```

The other Loader steps use the ordinary three-parameter `ExecuteAsync` override.

## Consequences of the chosen domain

The base class, not the exception type or the fact that a step performs I/O, is authoritative. A destination-adapter exception in Transactional Integration's `SendStep` becomes a business failure; an outbound exception in Extractor's or Loader's `SendStep` becomes a technical failure.

This assignment determines the result the next layer sees. Whether it becomes an incident, a retry, or an acknowledged message depends on finalizers and hosting. See [incident routing and broker retry](result-types.md#incident-routing-and-broker-retry) before assuming all business failures consume messages or all technical failures are retried automatically.

## Related

- [Implementing pipeline steps](../implementing-pipeline-steps.md) — a complete validator and pipeline call
- [Steps](../core/steps.md) — signatures, context mutation, and finalizer triggers
- [Results](../core/results.md) — nested cases and failure payload construction
- [Result Types](result-types.md) — operational meaning and cancellation
