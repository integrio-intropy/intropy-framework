# Steps

> Abstract base classes and override signatures for pipeline steps.

**Namespace:** `Intropy.Framework.Core.Pipeline.Abstractions.Steps`

**Assembly:** `Intropy.Framework.Core`

For a complete implementation, start with [Implementing pipeline steps](../implementing-pipeline-steps.md). For the named block abstractions, use the [block-specific matrix](../concepts/step-types.md#block-step-abstractions); identically named steps can have different failure domains.

## Step\<TIn, TOut, TCtx\>

Generic step with no predefined failure domain. Public abstract members:

```csharp
public abstract class Step<TIn, TOut, TCtx>
{
    public abstract string StepName { get; }
    public abstract Task<(StepResult<TOut> Result, TCtx Context)> ExecuteAsync(
        TIn input, TCtx context, CancellationToken ct);
}
```

Return any of the five `StepResult<TOut>` cases. Ordinary uncaught exceptions become `StepResult<TOut>.TechnicalFailure` when executed through the pipeline.

## BusinessStep\<TIn, TOut, TCtx\>

Public abstract members:

```csharp
public abstract class BusinessStep<TIn, TOut, TCtx>
{
    public abstract string StepName { get; }
    public abstract Task<(BusinessStepResult<TOut> Result, TCtx Context)> ExecuteAsync(
        TIn input, TCtx context, CancellationToken ct);
}
```

Return `Success`, `Cancelled`, `Failure(BusinessIncidentData)`, or `Aborted` from `BusinessStepResult<TOut>`. Ordinary uncaught exceptions become a business failure containing `BusinessIncidentData`.

## TechnicalStep\<TIn, TOut, TCtx\>

Public abstract members:

```csharp
public abstract class TechnicalStep<TIn, TOut, TCtx>
{
    public abstract string StepName { get; }
    public abstract Task<(TechnicalStepResult<TOut> Result, TCtx Context)> ExecuteAsync(
        TIn input, TCtx context, CancellationToken ct);
}
```

Return `Success`, `Cancelled`, `Failure(TechnicalFailure)`, or `Aborted` from `TechnicalStepResult<TOut>`. Ordinary uncaught exceptions become a technical failure.

### Execution contract

- `StepName` names the tracing span (`Step.{StepName}`). Named block abstractions already override it.
- `input` is the previous successful step's value. Non-success results skip subsequent ordinary steps.
- Return **both** the result and the context. The pipeline passes the returned context to the next step; it does not require a new context instance.
- `CancellationToken ct` is required in these overrides. Forward it to APIs that support cancellation.
- The pipeline wrapper returns `Aborted` if the token is already signalled, or if `ExecuteAsync` throws `OperationCanceledException` while that token is signalled. Other exceptions follow the step's failure domain.
- Calling a step's public `ExecuteAsync` directly bypasses the pipeline's exception handling and tracing wrapper. This matters in unit tests.

Step methods return a **two-element** tuple. The low-level `Pipeline.Start(...).AddStep(...)` chain carries a **three-element** tuple: `(Result, Context, CancellationToken)`. Block `Execute` methods return `(Result, Context)`.

## Finalizer\<T, TCtx\>

Receives the complete current result, not just its success value. Public abstract members:

```csharp
public abstract class Finalizer<T, TCtx>
{
    public abstract string FinalizerName { get; }
    public abstract FinalizerTrigger Triggers { get; }
    public abstract Task<(StepResult<T> Result, TCtx Context)> ExecuteAsync(
        StepResult<T> result, TCtx context, CancellationToken ct);
}
```

`FinalizerName` names the tracing span (`Finalizer.{FinalizerName}`). `Triggers` selects the result cases for which the pipeline evaluates this finalizer. The returned result and context replace the current ones; for example, an incident router can turn `BusinessFailure` into `Success` after routing.

Finalizers use the same cancellation checks as steps. An already-signalled token prevents the body from running, even when `OnAborted` matches. Do not rely on `OnAborted` for cleanup that must run during shutdown.

## FinalizerTrigger

**Namespace:** `Intropy.Framework.Core.Pipeline.Abstractions.Enums`

```csharp
[Flags]
public enum FinalizerTrigger
{
    OnSuccess = 1,
    OnCancelled = 2,
    OnBusinessFailure = 4,
    OnTechnicalFailure = 8,
    OnAborted = 16
}
```

Combine flags with `|`, for example `FinalizerTrigger.OnSuccess | FinalizerTrigger.OnBusinessFailure`.

## Context

**Namespace:** `Intropy.Framework.Blocks.Shared`

**Assembly:** `Intropy.Framework.Blocks`

```csharp
public record Context(Dictionary<string, string> Metadata, bool IsRetry = false);
```

Block step abstractions constrain `TCtx : Context`; the Core step classes themselves do not require this type. `IsRetry` marks a retry and defaults to `false`.

**The record does not make its dictionary immutable.** `Metadata` is a mutable dictionary held by reference. Writing `context.Metadata["order_id"] = orderId` and returning that same `context` carries the write downstream. No context reconstruction is needed.

A record `with` expression makes a shallow copy: `context with { IsRetry = true }` still shares the dictionary. To isolate metadata, explicitly copy it, e.g. `context with { Metadata = new Dictionary<string, string>(context.Metadata) }`. If a step returns a replacement context with a different dictionary, subsequent steps receive that dictionary instead. Do not share a mutable dictionary between independently executing messages.

The pipeline does not undo metadata mutations when a step fails. Returning a failure is not a rollback mechanism.

## IHashable

**Namespace:** `Intropy.Framework.Core.Pipeline.Abstractions`

```csharp
public interface IHashable
{
    string GetHashString();
}
```

Opt-in interface for types that provide a custom hash for idempotency checks. `ExternalIdempotencyChecker` SHA256-hashes the UTF-8 bytes of `GetHashString()` and Base64-encodes the digest. Without `IHashable`, it hashes the JSON serialization instead. An explicit builder `hashGenerator` bypasses both paths and its return value is used directly. See [hash generation](../blocks/loader.md#hash-generation).

## Source and related reference

- [Step base classes](../../src/Intropy.Framework.Core/Pipeline/Abstractions/Steps/)
- [Pipeline.cs](../../src/Intropy.Framework.Core/Pipeline/Core/Pipeline.cs) — context hand-off and finalizer dispatch
- [Context.cs](../../src/Intropy.Framework.Blocks/Shared/Context.cs)
- [FinalizerTrigger.cs](../../src/Intropy.Framework.Core/Pipeline/Abstractions/Enums/FinalizerTrigger.cs)
- [Results](results.md) — nested cases and payload construction
- [Step Types](../concepts/step-types.md) — block-specific domains and extension points
- [Builders](builders.md) — wiring steps into block pipelines
