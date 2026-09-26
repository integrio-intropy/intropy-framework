# Observability

> Every step and pipeline gets automatic OpenTelemetry tracing with result classification.

## How it works

```mermaid
graph TD
    P["Pipeline.MyPipeline"] --> S1["Step.Deserialize"]
    P --> S2["Step.Validate"]
    P --> S3["Step.Transform"]
    P --> F["Finalizer.RouteBusinessIncidents"]
```

The framework creates OpenTelemetry `Activity` spans at two levels: one for the entire pipeline and one for each individual step. Spans are created automatically — you don't need to add any tracing code to your step implementations.

## Pipeline-level tracing

Wrap any pipeline with `PipelineTracing.ExecuteWithTracing` to create a parent span:

```csharp
var (result, context) = await PipelineTracing.ExecuteWithTracing(
    pipeline: () => Pipeline.Start(input, ctx)
        .AddStep(step1)
        .AddStep(step2)
        .AddFinalizer(finalizer),
    pipelineName: "order-pipeline",
    logger: logger);
```

The pipeline span:

- Name: `Pipeline.{pipelineName}`
- Tag: `pipeline.name` = the pipeline name
- Status: `Ok` on success/cancelled/business failure, `Error` on technical failure
- Logs technical failures via the provided `ILogger`

The Block pipelines (`SendPipeline`, `ReceivePipeline`, `Extractor`) use `ExecuteWithTracing` internally — you get pipeline-level tracing automatically.

### Detached traces

By default, the pipeline span is a child of the current activity. Set `detachTrace: true` to create a new root span linked to the parent instead:

```csharp
var (result, context) = await PipelineTracing.ExecuteWithTracing(
    pipeline: () => myPipeline(),
    pipelineName: "order-pipeline",
    logger: logger,
    detachTrace: true);
```

This is useful when you want a separate trace for each pipeline execution while preserving the causal link to the parent.

## Step-level tracing

Every step creates its own `Activity` span inside `ExecuteAsyncInternal`, which wraps your `ExecuteAsync` method. This happens automatically for all step types.

### Step spans

- Name: `Step.{StepName}`
- Tags:
    - `step.name` = the step's `StepName` property
    - `step.result` = one of `success`, `cancelled`, `business_failure`, `technical_failure`
    - `step.business_failure_message` = description (on business failure)
    - `step.technical_failure_message` = description (on technical failure)
- Status: `Ok` for all results except `technical_failure`, which sets `Error`

### Finalizer spans

- Name: `Finalizer.{FinalizerName}`
- Tags:
    - `finalizer.name` = the finalizer's `FinalizerName` property
    - `finalizer.triggers` = the configured trigger flags (e.g., `OnSuccess, OnBusinessFailure`)
    - `finalizer.output_result` = the result type after finalizer execution
- Status: same as step spans

## Exception handling and tracing

When an uncaught exception occurs in a step:

1. The span status is set to `Error`
2. The exception is added to the span via `AddException`
3. The exception is converted to the appropriate failure type:
    - `Step` and `TechnicalStep`: becomes `TechnicalFailure`
    - `BusinessStep`: becomes `BusinessIncident`

This means you can find failed steps in your tracing backend by filtering on `status = Error` or by looking at the `step.result` tag.

## Activity sources

The framework uses a separate `ActivitySource` for each package, versioned with the package:

| ActivitySource | Spans |
|----------------|-------|
| `Intropy.Framework.Core` | Pipelines and steps |
| `Intropy.Framework.Blocks` | Block-level operations |
| `Intropy.Framework.Adapters` | File adapter operations (list, get, create, delete) |
| `Intropy.Framework.Hosting` | Run-to-completion jobs, one trace per swept file, and consumed messages |

To collect traces, subscribe to all of them with the `IntropyTelemetry.ActivitySources` wildcard:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddSource(IntropyTelemetry.ActivitySources));
```

### Messaging spans

A Transactional Integration traces its queue hop with the OpenTelemetry messaging conventions.
The receive side publishes each file under a `send {topic}` span (kind `Producer`), and that span's
context travels with the message. The send side processes each message under a `process {topic}`
span (kind `Consumer`) that continues it. When a message carries no trace context, its consumer
span starts a new trace instead. Both carry `messaging.system`, `messaging.destination.name`,
`messaging.operation.type` and `messaging.message.id`. A failure sets `error.type` and `Error`
status. Each consumer span links to the job span of the run that consumed it. The job span is
tagged with the run's message counts: `intropy.messages.processed`, `intropy.messages.failed` (left
for redelivery) and `intropy.messages.skipped` (duplicates).

### Run-to-completion jobs

A job's process exits as soon as the job ends. Telemetry that is still buffered in the
exporter when the process exits is lost, and the job's own span is always in the last batch. Run
the job with `RunToCompletionAsync()` on the host: it starts the host (the OpenTelemetry hosting
integration creates its providers in a hosted service) and disposes it afterwards, which flushes
the exporters.

```csharp
return await app.RunToCompletionAsync();
```

If you build a bare `ServiceProvider` instead of a host, register the providers yourself (for
example `Sdk.CreateTracerProviderBuilder()`), and dispose them before returning.

## Practical implications

- You don't need to add tracing code to your steps — the framework handles it.
- Use `StepName` and `FinalizerName` properties to give your steps meaningful names that appear in traces.
- Technical failures show as `Error` status in your tracing backend; business failures show as `Ok` status with a `step.result = business_failure` tag. This distinction lets you set up different alerting rules for each.
- The `pipeline.name` tag on the root span lets you filter and group traces by integration pipeline.

## Related

- [Pipeline Execution](pipeline-execution.md) — how steps chain together
- [Pipeline](../core/pipeline.md) — `PipelineTracing.ExecuteWithTracing` signature
