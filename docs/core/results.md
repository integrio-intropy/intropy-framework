# Results

> Result records returned by steps and pipelines, with their failure payloads.

**Namespace:** `Intropy.Framework.Core.Pipeline.Abstractions.Results`

**Assembly:** `Intropy.Framework.Core`

These declarations describe the source at this documentation revision. Use the documentation from the tag or commit corresponding to your installed framework packages, not an unrelated branch. Start with [Implementing pipeline steps](../implementing-pipeline-steps.md) for a working example.

## Result families

| Returned by | Result type | Nested cases | Failure payload |
|---|---|---|---|
| `BusinessStep<TIn, TOut, TCtx>` | `BusinessStepResult<TOut>` | `Success`, `Cancelled`, `Failure`, `Aborted` | `BusinessIncidentData` |
| `TechnicalStep<TIn, TOut, TCtx>` | `TechnicalStepResult<TOut>` | `Success`, `Cancelled`, `Failure`, `Aborted` | `TechnicalFailure` |
| `Step<TIn, TOut, TCtx>`, finalizers, pipelines | `StepResult<T>` | `Success`, `Cancelled`, `BusinessFailure`, `TechnicalFailure`, `Aborted` | The corresponding payload below |

The nested cases are sealed records on an abstract generic record. Construct a case, not the abstract base. A business step returns `BusinessStepResult<T>.Failure`, not `StepResult<T>.BusinessFailure`.

## StepResult\<T\>

Public outcome declarations:

```csharp
public abstract record StepResult<T>
{
    public sealed record Success(T Value) : StepResult<T>;
    public sealed record Cancelled : StepResult<T>;
    public sealed record BusinessFailure(
        Intropy.Contracts.BusinessIncidentService.BusinessIncidentData Value) : StepResult<T>;
    public sealed record TechnicalFailure(
        Intropy.Framework.Core.Pipeline.Abstractions.Failures.TechnicalFailure Value) : StepResult<T>;
    public sealed record Aborted : StepResult<T>;
}
```

| Case | Meaning | Effect on subsequent ordinary steps |
|---|---|---|
| `Success(T Value)` | Completed with an output value | Execute the next step |
| `Cancelled` | Logical stop, such as an idempotency duplicate | Skip |
| `BusinessFailure(BusinessIncidentData Value)` | Business-domain failure | Skip |
| `TechnicalFailure(TechnicalFailure Value)` | Technical-domain failure | Skip |
| `Aborted` | Execution aborted, normally by cancellation-token signalling | Skip |

Finalizers are evaluated separately using their [trigger flags](steps.md#finalizertrigger). They can replace the current result. Failure classification alone does not route an incident or acknowledge a broker message; see [routing and retry](../concepts/result-types.md#incident-routing-and-broker-retry).

## BusinessStepResult\<T\>

Public outcome declarations:

```csharp
public abstract record BusinessStepResult<T>
{
    public sealed record Success(T Value) : BusinessStepResult<T>;
    public sealed record Cancelled : BusinessStepResult<T>;
    public sealed record Failure(
        Intropy.Contracts.BusinessIncidentService.BusinessIncidentData Value) : BusinessStepResult<T>;
    public sealed record Aborted : BusinessStepResult<T>;
}
```

The pipeline engine converts `Success`, `Cancelled`, and `Aborted` to the same-named `StepResult<T>` cases; `Failure` becomes `StepResult<T>.BusinessFailure` with the same payload. `BusinessStepResult<T>.ToStepResult()` is **internal**, not a consumer API.

## TechnicalStepResult\<T\>

Public outcome declarations:

```csharp
public abstract record TechnicalStepResult<T>
{
    public sealed record Success(T Value) : TechnicalStepResult<T>;
    public sealed record Cancelled : TechnicalStepResult<T>;
    public sealed record Failure(
        Intropy.Framework.Core.Pipeline.Abstractions.Failures.TechnicalFailure Value) : TechnicalStepResult<T>;
    public sealed record Aborted : TechnicalStepResult<T>;
}
```

This type also exposes `public StepResult<T> ToStepResult()`. The pipeline calls it automatically: `Failure` becomes `StepResult<T>.TechnicalFailure`; the other cases retain their names. Step implementations normally do not need to call it.

## BusinessIncidentData

**Namespace:** `Intropy.Contracts.BusinessIncidentService`

**Package:** `Intropy.Contracts` (the version is pinned in [Directory.Packages.props](../../Directory.Packages.props))

Construct the business failure payload with an object initializer, not the old `BusinessIncident(description, occurredAt, context)` constructor:

```csharp
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

BusinessStepResult<string> result = new BusinessStepResult<string>.Failure(
    new BusinessIncidentData
    {
        Description = "Order ID is required",
        Context = new Dictionary<string, string> { ["field"] = "orderId" }
    });
```

`Description` describes the problem; `Context` carries diagnostic fields for the incident. This payload dictionary is distinct from the pipeline's `Context.Metadata` unless you deliberately share it.

## TechnicalFailure

**Namespace:** `Intropy.Framework.Core.Pipeline.Abstractions.Failures`

**Assembly:** `Intropy.Framework.Core`

Constructor signature (attributes omitted):

```csharp
public record TechnicalFailure(
    string Description,
    string? ErrorMessage = null,
    Exception? Exception = null,
    params object?[]? ErrorMessageArgs);
```

| Parameter | Meaning |
|---|---|
| `Description` | Short description of the failure |
| `ErrorMessage` | Structured logging template, e.g. `"Could not connect to {host}"` |
| `Exception` | Optional underlying exception |
| `ErrorMessageArgs` | Arguments for the structured logging template |

```csharp
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

TechnicalStepResult<string> result = new TechnicalStepResult<string>.Failure(
    new TechnicalFailure(
        Description: "Destination unavailable",
        ErrorMessage: "Could not connect to {host}",
        ErrorMessageArgs: ["orders.example.com"]));
```

## Handling every pipeline outcome

```csharp
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

static string Describe(StepResult<string> result) => result switch
{
    StepResult<string>.Success s => $"Processed: {s.Value}",
    StepResult<string>.Cancelled => "Skipped (for example, a duplicate)",
    StepResult<string>.BusinessFailure bf => $"Business issue: {bf.Value.Description}",
    StepResult<string>.TechnicalFailure tf => $"Technical error: {tf.Value.Description}",
    StepResult<string>.Aborted => "Execution aborted",
    _ => throw new InvalidOperationException("Unknown result type")
};
```

`Cancelled` and `Aborted` are parameterless: construct them with `new StepResult<T>.Cancelled()` and `new StepResult<T>.Aborted()`. The same applies to the step-level families.

## Source and related reference

- [StepResult.cs](../../src/Intropy.Framework.Core/Pipeline/Abstractions/Results/StepResult.cs)
- [BusinessStepResult.cs](../../src/Intropy.Framework.Core/Pipeline/Abstractions/Results/BusinessStepResult.cs)
- [TechnicalStepResult.cs](../../src/Intropy.Framework.Core/Pipeline/Abstractions/Results/TechnicalStepResult.cs)
- [TechnicalFailure.cs](../../src/Intropy.Framework.Core/Pipeline/Abstractions/Failures/TechnicalFailure.cs)
- [Steps](steps.md) — override signatures and context semantics
- [Result Types](../concepts/result-types.md) — cancellation, incident routing, and retry
