# Pipeline Execution

> The pipeline engine chains typed steps together using extension methods, propagating failures automatically.

## How it works

```mermaid
graph LR
    S[Start] --> A[Step A]
    A -->|Success| B[Step B]
    B -->|Success| C[Step C]
    C -->|Success| F[Finalizer]
    A -->|Failure| F
    B -->|Failure| F
```

A pipeline starts with `Pipeline.Start(value, context)`, which wraps the initial value as a `StepResult<T>.Success`. Each subsequent call to `.AddStep(step)` is an extension method on `Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)>`. The compiler enforces that the output type of one step matches the input type of the next.

Steps only execute when the previous result is `Success`. If any step returns `Cancelled`, `BusinessFailure`, `TechnicalFailure`, or `Aborted`, the failure propagates through all remaining steps unchanged — they are skipped without execution. Finalizers are the exception: they run conditionally based on their `FinalizerTrigger` flags.

## Building a pipeline

The following composition snippets are illustrative: step classes and input variables belong to your application. For a complete runnable example, see [Implementing pipeline steps](../implementing-pipeline-steps.md).

The pipeline API is a chain of extension methods. There is no configuration object or runtime pipeline builder — the pipeline shape is determined at compile time:

```csharp
var (result, context, _) = await Pipeline.Start(inputValue, myContext)
    .AddStep(new DeserializeStep())
    .AddStep(new ValidateStep())
    .AddStep(new TransformStep())
    .AddFinalizer(new CleanupFinalizer());
```

The generic type parameters flow through the chain: if `DeserializeStep` is `Step<string, Order, MyContext>` and `ValidateStep` is `Step<Order, Order, MyContext>`, the compiler checks that `Order` matches at the boundary. A type mismatch is a compile error.

## Failure propagation

When a step returns a non-success result, the pipeline wraps it in the appropriate result type for the next step's output type and passes it through:

```mermaid
graph TD
    A["Step A: string → Order"] -->|"Success(order)"| B["Step B: Order → Order"]
    B -->|"BusinessFailure(incident)"| C["Step C: Order → Invoice"]
    C -->|"Skipped — BusinessFailure propagated"| D["Finalizer"]
```

The context from the last step that actually executed is preserved. Steps that are skipped don't modify the context.

## Adding multiple same-typed steps

Use `AddSteps` when you have multiple steps with the same input and output type (e.g., multiple extract or validation steps):

```csharp
var extractors = new List<BusinessStep<Order, Order, Context>>
{
    new EnrichWithCustomerData(),
    new EnrichWithPricingData()
};

var (result, context, _) = await Pipeline.Start(order, myContext)
    .AddSteps(extractors)  // executes in order, short-circuits on failure
    .AddStep(new TransformStep());
```

`AddSteps` is available for both `BusinessStep<T, T, TCtx>` and `TechnicalStep<T, T, TCtx>` collections. Steps execute in order and short-circuit on the first failure.

## Finalizers

Finalizers run conditionally based on `FinalizerTrigger` flags. Unlike regular steps, a finalizer receives the full `StepResult<T>` — not just the success value — so it can inspect and act on failures:

```csharp
public class NotificationFinalizer : Finalizer<string, Context>
{
    public override string FinalizerName => "Notify";

    public override FinalizerTrigger Triggers =>
        FinalizerTrigger.OnSuccess | FinalizerTrigger.OnBusinessFailure;

    public override Task<(StepResult<string> Result, Context Context)> ExecuteAsync(
        StepResult<string> result, Context context, CancellationToken ct)
    {
        // Runs on success AND business failure, but not on cancelled or technical failure
        return Task.FromResult((result, context));
    }
}
```

The available triggers are flags that can be combined:

| Flag | Runs when |
|------|-----------|
| `FinalizerTrigger.OnSuccess` | Pipeline completed successfully |
| `FinalizerTrigger.OnCancelled` | Pipeline was cancelled (idempotency) |
| `FinalizerTrigger.OnBusinessFailure` | A business step returned a failure |
| `FinalizerTrigger.OnTechnicalFailure` | A technical failure occurred |
| `FinalizerTrigger.OnAborted` | Execution was aborted |

A finalizer can replace both the result and context, including turning a handled failure into success. The engine does not preserve an earlier business failure if the finalizer returns a different result.

Trigger selection is not a cleanup guarantee: an already-signalled cancellation token prevents a finalizer body from running, including `OnAborted`. See [Steps](../core/steps.md#finalizertrigger).

## Tracing

Every pipeline can be wrapped with OpenTelemetry tracing using `PipelineTracing.ExecuteWithTracing`:

```csharp
var (result, context) = await PipelineTracing.ExecuteWithTracing(
    pipeline: () => Pipeline.Start(input, ctx)
        .AddStep(step1)
        .AddStep(step2),
    pipelineName: "my-pipeline",
    logger: logger);
```

This creates a parent Activity span for the entire pipeline. Each step also creates its own child span with `step.name` and `step.result` tags. See [Observability](observability.md) for details.

## Practical implications

- You do not need failure guards between steps — the engine handles propagation. Your step still owns input validation.
- Use finalizers to handle selected outcomes, subject to the cancellation contract above.
- Design steps to be safe for reuse: the framework does not enforce statelessness. TI DI registrations are singletons; avoid mutable per-message instance fields and use an isolated context per message.

## Related

- [Result Types](result-types.md) — the five result variants and when to use each
- [Step Types](step-types.md) — choosing between Step, BusinessStep, TechnicalStep, and Finalizer
- [Pipeline API Reference](../core/pipeline.md) — exact method signatures
