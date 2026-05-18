# Pipeline

> The core pipeline engine and tracing utilities.

**Namespace:** `Intropy.Framework.Core.Pipeline.Core`
**Assembly:** `Intropy.Framework.Core`

## Pipeline

Static class that provides the pipeline execution mechanism through extension methods.

### Start

```csharp
public static Task<(StepResult<T> Result, TCtx Context)> Start<T, TCtx>(T value, TCtx context)
```

Entry point for a pipeline. Wraps the value as `StepResult<T>.Success` and returns it with the context.

**Example:**

```csharp
var (result, context) = await Pipeline.Start("hello", new Context(new Dictionary<string, string>()));
// result is StepResult<string>.Success("hello")
```

### AddStep (Step)

```csharp
public static async Task<(StepResult<TOut> Result, TCtx Context)> AddStep<TIn, TOut, TCtx>(
    this Task<(StepResult<TIn> Result, TCtx Context)> input,
    Step<TIn, TOut, TCtx> step)
```

Adds a generic `Step` to the pipeline. Executes only if the previous result is `Success`. Failures propagate unchanged.

### AddStep (BusinessStep)

```csharp
public static async Task<(StepResult<TOut> Result, TCtx Context)> AddStep<TIn, TOut, TCtx>(
    this Task<(StepResult<TIn> Result, TCtx Context)> input,
    BusinessStep<TIn, TOut, TCtx> step)
```

Adds a `BusinessStep` to the pipeline. The `BusinessStepResult<TOut>` is automatically converted to `StepResult<TOut>`.

### AddStep (TechnicalStep)

```csharp
public static async Task<(StepResult<TOut> Result, TCtx Context)> AddStep<TIn, TOut, TCtx>(
    this Task<(StepResult<TIn> Result, TCtx Context)> input,
    TechnicalStep<TIn, TOut, TCtx> step)
```

Adds a `TechnicalStep` to the pipeline. The `TechnicalStepResult<TOut>` is automatically converted to `StepResult<TOut>`.

### AddSteps (BusinessStep)

```csharp
public static async Task<(StepResult<T> Result, TCtx Context)> AddSteps<T, TCtx>(
    this Task<(StepResult<T> Result, TCtx Context)> input,
    IEnumerable<BusinessStep<T, T, TCtx>> steps)
```

Adds multiple `BusinessStep` instances with the same input/output type. Steps execute in order and short-circuit on the first failure.

### AddSteps (TechnicalStep)

```csharp
public static async Task<(StepResult<T> Result, TCtx Context)> AddSteps<T, TCtx>(
    this Task<(StepResult<T> Result, TCtx Context)> input,
    IEnumerable<TechnicalStep<T, T, TCtx>> steps)
```

Adds multiple `TechnicalStep` instances with the same input/output type.

### AddFinalizer

```csharp
public static async Task<(StepResult<T> Result, TCtx Context)> AddFinalizer<T, TCtx>(
    this Task<(StepResult<T> Result, TCtx Context)> input,
    Finalizer<T, TCtx> finalizer)
```

Adds a `Finalizer` to the pipeline. Executes only if the current result matches the finalizer's `Triggers` flags. If the finalizer produces a new `BusinessFailure` but one already occurred earlier, the first `BusinessFailure` is preserved.

---

## PipelineTracing

Static class for wrapping pipelines with OpenTelemetry distributed tracing.

**Namespace:** `Intropy.Framework.Core.Pipeline.Core`

### ExecuteWithTracing

```csharp
public static async Task<(StepResult<T> Result, TCtx Context)> ExecuteWithTracing<T, TCtx>(
    Func<Task<(StepResult<T> Result, TCtx Context)>> pipeline,
    string pipelineName,
    ILogger logger,
    Action<Activity?>? configureActivity = null,
    bool detachTrace = false)
```

Executes a pipeline within an OpenTelemetry Activity span.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pipeline` | `Func<Task<...>>` | Factory that creates and executes the pipeline |
| `pipelineName` | `string` | Name for the Activity span (`Pipeline.{pipelineName}`) |
| `logger` | `ILogger` | Logger for technical failure logging |
| `configureActivity` | `Action<Activity?>?` | Optional callback to set additional Activity tags |
| `detachTrace` | `bool` | When `true`, creates a new root span linked to the parent |

**Example:**

```csharp
var (result, context) = await PipelineTracing.ExecuteWithTracing(
    () => Pipeline.Start(input, ctx)
        .AddStep(deserializer)
        .AddStep(transformer),
    "order-pipeline",
    logger,
    detachTrace: true);
```

---

## FrameworkOptions

**Namespace:** `Intropy.Framework.Core.Configuration`

```csharp
public class FrameworkOptions
{
    [Required]
    public required string ComponentName { get; set; }
}
```

Identifies the component for idempotency and business incident routing.

### Registration

```csharp
// Option 1: Explicit configuration
services.AddIntropyFramework(opts => opts.ComponentName = "my-integration");

// Option 2: Environment variable (INTROPY_COMPONENT_NAME)
services.AddIntropyFramework();
```

## See also

- [Pipeline Execution](../concepts/pipeline-execution.md) — how the pipeline engine works
- [Observability](../concepts/observability.md) — tracing details
