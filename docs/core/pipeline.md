# Pipeline

> The core pipeline engine and tracing utilities.

**Namespace:** `Intropy.Framework.Core.Pipeline.Core`
**Assembly:** `Intropy.Framework.Core`

## Pipeline

Static class that provides the pipeline execution mechanism through extension methods.

### Start

```csharp
public static Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> Start<T, TCtx>(
    T value, TCtx context, CancellationToken ct = default)
```

Entry point for a pipeline. Wraps the value as `StepResult<T>.Success` and returns it with the context and token. `Start` itself does not inspect cancellation; step wrappers do. Direct Core chains return three tuple elements; step overrides, block `Execute` methods, and `ExecuteWithTracing` return two.

**Example:**

```csharp
var (result, context, _) = await Pipeline.Start("hello", new Context(new Dictionary<string, string>()));
// result is StepResult<string>.Success("hello")
```

### AddStep (Step)

```csharp
public static async Task<(StepResult<TOut> Result, TCtx Context, CancellationToken CancellationToken)> AddStep<TIn, TOut, TCtx>(
    this Task<(StepResult<TIn> Result, TCtx Context, CancellationToken CancellationToken)> input,
    Step<TIn, TOut, TCtx> step)
```

Adds a generic `Step` to the pipeline. Executes only if the previous result is `Success`. Failures propagate unchanged.

### AddStep (BusinessStep)

```csharp
public static async Task<(StepResult<TOut> Result, TCtx Context, CancellationToken CancellationToken)> AddStep<TIn, TOut, TCtx>(
    this Task<(StepResult<TIn> Result, TCtx Context, CancellationToken CancellationToken)> input,
    BusinessStep<TIn, TOut, TCtx> step)
```

Adds a `BusinessStep` to the pipeline. The `BusinessStepResult<TOut>` is automatically converted to `StepResult<TOut>`.

### AddStep (TechnicalStep)

```csharp
public static async Task<(StepResult<TOut> Result, TCtx Context, CancellationToken CancellationToken)> AddStep<TIn, TOut, TCtx>(
    this Task<(StepResult<TIn> Result, TCtx Context, CancellationToken CancellationToken)> input,
    TechnicalStep<TIn, TOut, TCtx> step)
```

Adds a `TechnicalStep` to the pipeline. The `TechnicalStepResult<TOut>` is automatically converted to `StepResult<TOut>`.

### AddSteps (BusinessStep)

```csharp
public static async Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> AddSteps<T, TCtx>(
    this Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> input,
    IEnumerable<BusinessStep<T, T, TCtx>> steps)
```

Adds multiple `BusinessStep` instances with the same input/output type. Steps execute in order and short-circuit on the first failure.

### AddSteps (TechnicalStep)

```csharp
public static async Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> AddSteps<T, TCtx>(
    this Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> input,
    IEnumerable<TechnicalStep<T, T, TCtx>> steps)
```

Adds multiple `TechnicalStep` instances with the same input/output type.

### AddFinalizer

```csharp
public static async Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> AddFinalizer<T, TCtx>(
    this Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> input,
    Finalizer<T, TCtx> finalizer)
```

Adds a `Finalizer` to the pipeline. Executes only if the current result matches the finalizer's `Triggers` flags. Its returned result and context replace the current ones; there is no first-business-failure preservation. A signalled token prevents the finalizer body from running, even when `OnAborted` matches. See the [finalizer contract](steps.md).

### AddOptionalStep / AddOptionalFinalizer

`AddOptionalStep<T, TCtx>` accepts a nullable `Step<T, T, TCtx>`, `BusinessStep<T, T, TCtx>`, or `TechnicalStep<T, T, TCtx>`. `AddOptionalFinalizer<T, TCtx>` accepts a nullable `Finalizer<T, TCtx>`. Both take and return the same three-element chain as the methods above. A null argument passes the chain through unchanged; a non-null argument delegates to `AddStep` or `AddFinalizer`. Optional steps must have identical input and output types.

---

## PipelineTracing

Static class for wrapping pipelines with OpenTelemetry distributed tracing.

**Namespace:** `Intropy.Framework.Core.Pipeline.Core`

### ExecuteWithTracing

```csharp
public static async Task<(StepResult<T> Result, TCtx Context)> ExecuteWithTracing<T, TCtx>(
    Func<Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)>> pipeline,
    string pipelineName,
    ILogger logger,
    Action<Activity?>? configureActivity = null,
    bool detachTrace = false)
```

Executes a three-element Core chain within an OpenTelemetry Activity span and returns `(Result, Context)`. Exceptions escaping the chain are logged and rethrown, not converted into a returned failure.

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
    [Required]
    public required string ServiceNamespace { get; set; }
}
```

Identifies the component and organization. Together they form the business-incident source URN `urn:{ServiceNamespace}:{ComponentName}` and label idempotency records per component.

### Registration

```csharp
// Option 1: Explicit configuration
services.AddIntropyFramework(opts =>
{
    opts.ComponentName = "my-integration";
    opts.ServiceNamespace = "example";
});

// Option 2: Both INTROPY_COMPONENT_NAME and INTROPY_SERVICE_NAMESPACE
services.AddIntropyFramework();
```

Both options are required. With a delegate, options are validated on resolution and on host start. Without a delegate, missing environment variables throw during registration; there is no environment fallback for fields omitted by a delegate. `AddIntropyFramework` does not register logging, Dapr clients, adapters, or external service clients.

## See also

- [Pipeline Execution](../concepts/pipeline-execution.md) — how the pipeline engine works
- [Observability](../concepts/observability.md) — tracing details

Source: [Pipeline.cs](../../src/Intropy.Framework.Core/Pipeline/Core/Pipeline.cs), [PipelineTracing.cs](../../src/Intropy.Framework.Core/Pipeline/Core/PipelineTracing.cs), [configuration registration](../../src/Intropy.Framework.Core/Configuration/ServiceCollectionExtensions.cs).
