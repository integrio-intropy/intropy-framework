# Results

> Discriminated union result types for pipeline steps.

**Namespace:** `Intropy.Framework.Core.Pipeline.Abstractions.Results`
**Assembly:** `Intropy.Framework.Core`

## StepResult\<T\>

The universal result type that flows through the pipeline.

```csharp
public abstract record StepResult<T>
{
    public sealed record Success(T Value) : StepResult<T>;
    public sealed record Cancelled : StepResult<T>;
    public sealed record BusinessFailure(BusinessIncident Value) : StepResult<T>;
    public sealed record TechnicalFailure(Failures.TechnicalFailure Value) : StepResult<T>;
}
```

| Variant | Payload | Pipeline behavior |
|---------|---------|-------------------|
| `Success(T Value)` | The step's output value | Next step executes |
| `Cancelled` | None | Remaining steps skipped |
| `BusinessFailure(BusinessIncident Value)` | Business incident details | Remaining steps skipped |
| `TechnicalFailure(TechnicalFailure Value)` | Technical failure details | Remaining steps skipped |

**Creating results:**

```csharp
// In a Step<TIn, TOut, TCtx>
return (new StepResult<Order>.Success(order), context);
return (new StepResult<Order>.Cancelled(), context);
return (new StepResult<Order>.BusinessFailure(incident), context);
return (new StepResult<Order>.TechnicalFailure(failure), context);
```

**Pattern matching:**

```csharp
switch (result)
{
    case StepResult<string>.Success s:
        Console.WriteLine($"Value: {s.Value}");
        break;
    case StepResult<string>.Cancelled:
        Console.WriteLine("Already processed");
        break;
    case StepResult<string>.BusinessFailure bf:
        Console.WriteLine($"Business issue: {bf.Value.Description}");
        break;
    case StepResult<string>.TechnicalFailure tf:
        Console.WriteLine($"Technical error: {tf.Value.Description}");
        break;
}
```

---

## BusinessStepResult\<T\>

Result type for `BusinessStep<TIn, TOut, TCtx>`. Three variants — no `TechnicalFailure`.

```csharp
public abstract record BusinessStepResult<T>
{
    public sealed record Success(T Value) : BusinessStepResult<T>;
    public sealed record Cancelled : BusinessStepResult<T>;
    public sealed record Failure(BusinessIncident Value) : BusinessStepResult<T>;
}
```

Automatically converted to `StepResult<T>` by the pipeline engine after execution.

---

## TechnicalStepResult\<T\>

Result type for `TechnicalStep<TIn, TOut, TCtx>`. Three variants — no `BusinessFailure`.

```csharp
public abstract record TechnicalStepResult<T>
{
    public sealed record Success(T Value) : TechnicalStepResult<T>;
    public sealed record Cancelled : TechnicalStepResult<T>;
    public sealed record Failure(TechnicalFailure Value) : TechnicalStepResult<T>;
}
```

### ToStepResult

```csharp
public StepResult<T> ToStepResult()
```

Converts to the equivalent `StepResult<T>` variant. Called automatically by the pipeline engine.

---

## TechnicalFailure

**Namespace:** `Intropy.Framework.Core.Pipeline.Abstractions.Failures`

Payload for technical failures. Represents infrastructure or system-level issues.

```csharp
public record TechnicalFailure(
    string Description,
    string? ErrorMessage = null,
    Exception? Exception = null,
    params object?[]? ErrorMessageArgs);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `Description` | `string` | Short description of the failure |
| `ErrorMessage` | `string?` | Structured message template (e.g., `"Failed to connect to {host}"`) |
| `Exception` | `Exception?` | The exception that caused the failure |
| `ErrorMessageArgs` | `object?[]?` | Arguments for the structured message template |

**Example:**

```csharp
var failure = new TechnicalFailure(
    Description: "Database connection failed",
    ErrorMessage: "Could not connect to {database} at {host}",
    Exception: ex,
    ErrorMessageArgs: ["orders_db", "db.example.com"]);

return (new TechnicalStepResult<Order>.Failure(failure), context);
```

The framework uses `ErrorMessage` and `ErrorMessageArgs` for structured logging via `ILogger.LogError`.

---

## BusinessIncident

**Namespace:** `Intropy.Libs.Contracts.BusinessIncidentService`
**Assembly:** `Intropy.Libs.Contracts`

Payload for business failures. Defined in the external `Intropy.Libs.Contracts` package.

```csharp
public record BusinessIncident(
    string Description,
    DateTimeOffset OccurredAt,
    Dictionary<string, string> Context);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `Description` | `string` | Human-readable description of the issue |
| `OccurredAt` | `DateTimeOffset` | When the incident occurred |
| `Context` | `Dictionary<string, string>` | Additional context (message IDs, field values, etc.) |

## See also

- [Result Types](../concepts/result-types.md) — design rationale
- [Steps](steps.md) — which step types produce which results
