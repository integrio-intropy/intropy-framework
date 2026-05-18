# Steps

> Abstract base classes for pipeline steps. You subclass these to implement your integration logic.

**Namespace:** `Intropy.Framework.Core.Pipeline.Abstractions.Steps`
**Assembly:** `Intropy.Framework.Core`

## Step\<TIn, TOut, TCtx\>

Generic step with no predefined failure domain. Returns `StepResult<TOut>` directly. Uncaught exceptions become `TechnicalFailure`.

```csharp
public abstract class Step<TIn, TOut, TCtx>
{
    public abstract string StepName { get; }
    public abstract Task<(StepResult<TOut> Result, TCtx Context)> ExecuteAsync(TIn input, TCtx context);
}
```

### Required overrides

| Member | Type | Description |
|--------|------|-------------|
| `StepName` | `string` | Name used in tracing spans (`Step.{StepName}`) |
| `ExecuteAsync` | `Task<(StepResult<TOut>, TCtx)>` | Your step logic. Receives input value and context. |

---

## BusinessStep\<TIn, TOut, TCtx\>

Step whose failure domain is "business." Returns `BusinessStepResult<TOut>`. Uncaught exceptions become `BusinessIncident`.

```csharp
public abstract class BusinessStep<TIn, TOut, TCtx>
{
    public abstract string StepName { get; }
    public abstract Task<(BusinessStepResult<TOut> Result, TCtx Context)> ExecuteAsync(TIn input, TCtx context);
}
```

### Required overrides

| Member | Type | Description |
|--------|------|-------------|
| `StepName` | `string` | Name used in tracing spans |
| `ExecuteAsync` | `Task<(BusinessStepResult<TOut>, TCtx)>` | Return `Success`, `Cancelled`, or `Failure(BusinessIncident)` |

**Example:**

```csharp
public class OrderValidator : BusinessStep<Order, Order, Context>
{
    public override string StepName => "ValidateOrder";

    public override Task<(BusinessStepResult<Order> Result, Context Context)> ExecuteAsync(
        Order input, Context context)
    {
        if (input.Amount <= 0)
        {
            var incident = new BusinessIncident(
                "Invalid order amount",
                DateTimeOffset.UtcNow,
                new Dictionary<string, string> { ["orderId"] = input.OrderId });
            return Task.FromResult<(BusinessStepResult<Order>, Context)>(
                (new BusinessStepResult<Order>.Failure(incident), context));
        }

        return Task.FromResult<(BusinessStepResult<Order>, Context)>(
            (new BusinessStepResult<Order>.Success(input), context));
    }
}
```

---

## TechnicalStep\<TIn, TOut, TCtx\>

Step whose failure domain is "technical." Returns `TechnicalStepResult<TOut>`. Uncaught exceptions become `TechnicalFailure`.

```csharp
public abstract class TechnicalStep<TIn, TOut, TCtx>
{
    public abstract string StepName { get; }
    public abstract Task<(TechnicalStepResult<TOut> Result, TCtx Context)> ExecuteAsync(TIn input, TCtx context);
}
```

### Required overrides

| Member | Type | Description |
|--------|------|-------------|
| `StepName` | `string` | Name used in tracing spans |
| `ExecuteAsync` | `Task<(TechnicalStepResult<TOut>, TCtx)>` | Return `Success`, `Cancelled`, or `Failure(TechnicalFailure)` |

**Example:**

```csharp
public class OrderTransformer : TechnicalStep<Order, Invoice, Context>
{
    public override string StepName => "TransformOrder";

    public override Task<(TechnicalStepResult<Invoice> Result, Context Context)> ExecuteAsync(
        Order input, Context context)
    {
        var invoice = new Invoice($"INV-{input.OrderId}", input.CustomerName, input.Amount * 1.25m, DateTime.UtcNow);
        return Task.FromResult<(TechnicalStepResult<Invoice>, Context)>(
            (new TechnicalStepResult<Invoice>.Success(invoice), context));
    }
}
```

---

## Finalizer\<T, TCtx\>

A step that runs conditionally based on the pipeline result. Receives the full `StepResult<T>`, not just the success value.

```csharp
public abstract class Finalizer<T, TCtx>
{
    public abstract string FinalizerName { get; }
    public abstract FinalizerTrigger Triggers { get; }
    public abstract Task<(StepResult<T> Result, TCtx Context)> ExecuteAsync(StepResult<T> result, TCtx context);
}
```

### Required overrides

| Member | Type | Description |
|--------|------|-------------|
| `FinalizerName` | `string` | Name used in tracing spans (`Finalizer.{FinalizerName}`) |
| `Triggers` | `FinalizerTrigger` | Flags that determine when the finalizer runs |
| `ExecuteAsync` | `Task<(StepResult<T>, TCtx)>` | Receives the current result (any variant) and context |

---

## FinalizerTrigger

**Namespace:** `Intropy.Framework.Core.Pipeline.Abstractions.Enums`

```csharp
[Flags]
public enum FinalizerTrigger
{
    OnSuccess = 1,
    OnCancelled = 2,
    OnBusinessFailure = 4,
    OnTechnicalFailure = 8
}
```

Combine flags to run on multiple result types:

```csharp
public override FinalizerTrigger Triggers =>
    FinalizerTrigger.OnSuccess | FinalizerTrigger.OnBusinessFailure;
```

---

## Context

**Namespace:** `Intropy.Framework.Blocks.Shared`
**Assembly:** `Intropy.Framework.Blocks`

```csharp
public record Context(Dictionary<string, string> Metadata, bool IsRetry = false);
```

Flows through every step in a Block pipeline. `Metadata` is used for passing data between steps (idempotency keys, message IDs). `IsRetry` indicates whether the current message is being retried.

All Block step abstractions constrain `TCtx : Context`.

---

## IHashable

**Namespace:** `Intropy.Framework.Core.Pipeline.Abstractions`

```csharp
public interface IHashable
{
    string GetHashString();
}
```

Opt-in interface for types that want to provide a custom hash for idempotency checks. If a type implements `IHashable`, `ExternalIdempotencyChecker` uses `GetHashString()` instead of the default reflection-based SHA256 hash.

## See also

- [Step Types](../concepts/step-types.md) — when to use each step type
- [Results](results.md) — the result types each step produces
- [Builders](builders.md) — how steps are wired into block pipelines
