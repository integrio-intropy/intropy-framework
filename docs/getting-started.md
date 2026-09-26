# Getting Started

This guide walks you through building a complete Transactional Integration with Intropy Framework. By the end, you'll have a working two-pipeline system that reads source files, publishes them to Dapr pub/sub, and processes them through a send pipeline.

## Prerequisites

- .NET 10 SDK
- A .NET worker service project
- A running Dapr sidecar (for pub/sub at runtime)

## Install the packages

```bash
dotnet add package Intropy.Framework.Hosting
```

The Hosting package provides `AddTransactionalIntegration` and the `RunToCompletionRunner` that hosts it, and transitively brings in Blocks, Adapters, and Core. If you only need the pipeline engine without the TI lifecycle, install `Intropy.Framework.Blocks` (or just `Intropy.Framework.Core`) instead.

## Register framework services

In your `Program.cs`, register the core framework and Transactional Integration services:

```csharp
builder.Services.AddIntropyFramework(opts => opts.ComponentName = "order-processor");

builder.Services.AddTransactionalIntegration(opts =>
{
    opts.DaprPubSubName = "pubsub";
    opts.DaprTopicName = "orders";
});
```

`AddIntropyFramework` registers `FrameworkOptions` and validates the component name. You can also set the `INTROPY_COMPONENT_NAME` environment variable and call `AddIntropyFramework()` without a delegate.

`AddTransactionalIntegration` registers the lifecycle as a run-to-completion job, together with the `RunToCompletionRunner` that hosts it. `DaprPubSubName` and `DaprTopicName` are required.

## Define your data models

```csharp
public record Order(string OrderId, DateTime CreatedAt, string CustomerName, decimal Amount);

public record Invoice(string InvoiceNumber, string CustomerName, decimal Total, DateTime IssuedDate);
```

## The receive side

The framework owns the receive side. It sweeps the source for you: it lists the keyed
`IFileAdapter`, reads each file, publishes it to the integration's own topic
(`DaprPubSubName` / `DaprTopicName`) as a CloudEvent carrying the context metadata and trace
parent, and deletes the file (or archives it) only after it is on the queue. You only register the
source port, with its adapter configured under `Ports:order-source`:

```csharp
builder.Services.AddSourcePort("order-source", builder.Configuration);
// or, to archive handled files: AddSourcePort("order-source", builder.Configuration, SweepCompletion.Archive("archive"))
```

To replace the publisher, register your own `EnqueueStep<Context>`; to replace the whole receive
pipeline, register an `IReceivePipeline<Context>` (for example with `AddReceivePipeline`).

## Implement the send pipeline

The send pipeline processes messages from the pub/sub topic.

### Deserializer

`DeserializeStep<TInput, TCtx>` extends `BusinessStep<ReadOnlyMemory<byte>, TInput, TCtx>`:

```csharp
public class OrderDeserializer : DeserializeStep<Order, Context>
{
    public override Task<(BusinessStepResult<Order> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context)
    {
        var order = JsonSerializer.Deserialize<Order>(input.Span)!;
        return Task.FromResult<(BusinessStepResult<Order>, Context)>(
            (new BusinessStepResult<Order>.Success(order), context));
    }
}
```

### Validator

`ValidateStep<T, TCtx>` extends `BusinessStep<T, T, TCtx>`. Same type in and out — it either passes the value through or returns a business failure:

```csharp
public class OrderValidator : ValidateStep<Order, Context>
{
    public override Task<(BusinessStepResult<Order> Result, Context Context)> ExecuteAsync(
        Order input, Context context)
    {
        if (input.Amount <= 0)
        {
            var incident = new BusinessIncident(
                "Order validation failed: amount must be positive",
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

### Transformer

`TransformStep<TInput, TOutput, TCtx>` extends `TechnicalStep<TInput, TOutput, TCtx>`:

```csharp
public class OrderToInvoiceTransformer : TransformStep<Order, Invoice, Context>
{
    public override Task<(TechnicalStepResult<Invoice> Result, Context Context)> ExecuteAsync(
        Order input, Context context)
    {
        var invoice = new Invoice(
            InvoiceNumber: $"INV-{input.OrderId}",
            CustomerName: input.CustomerName,
            Total: input.Amount * 1.25m,
            IssuedDate: DateTime.UtcNow);

        return Task.FromResult<(TechnicalStepResult<Invoice>, Context)>(
            (new TechnicalStepResult<Invoice>.Success(invoice), context));
    }
}
```

### Serializer

`SerializeStep<T, TCtx>` extends `TechnicalStep<T, string, TCtx>`:

```csharp
public class InvoiceSerializer : SerializeStep<Invoice, Context>
{
    public override Task<(TechnicalStepResult<string> Result, Context Context)> ExecuteAsync(
        Invoice input, Context context)
    {
        var json = JsonSerializer.Serialize(input);
        return Task.FromResult<(TechnicalStepResult<string>, Context)>(
            (new TechnicalStepResult<string>.Success(json), context));
    }
}
```

### Sender

`SendStep<TCtx>` extends `BusinessStep<string, string, TCtx>`:

```csharp
public class InvoiceApiSender(HttpClient httpClient) : SendStep<Context>
{
    public override async Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
        string input, Context context)
    {
        var response = await httpClient.PostAsync("/invoices",
            new StringContent(input, Encoding.UTF8, "application/json"));
        response.EnsureSuccessStatusCode();

        return (new BusinessStepResult<string>.Success(input), context);
    }
}
```

### Register the send pipeline

```csharp
builder.Services.AddSendPipeline<Order, Invoice, Context>("order-send",
    (pipelineBuilder, sp) => pipelineBuilder
        .WithDeserializer(new OrderDeserializer())
        .WithValidator(new OrderValidator())
        .WithIdempotency(
            sp.GetRequiredService<IIdempotencyServiceClient>(),
            idExtractor: order => order.OrderId,
            dateExtractor: order => order.CreatedAt)
        .WithTransformer(new OrderToInvoiceTransformer())
        .WithSerializer(new InvoiceSerializer())
        .WithSender(sp.GetRequiredService<InvoiceApiSender>())
        .WithBusinessIncidents(
            sp.GetRequiredService<IBusinessIncidentServiceClient>(),
            messageIdExtractor: ctx => ctx.Metadata["message_id"]));
```

All builder methods are required. The builder throws `InvalidOperationException` at startup if any step is missing.

## Run it

The `RunToCompletionRunner` hosts the lifecycle. Run it through the host, and return its exit code:

```csharp
return await app.RunToCompletionAsync();
```

`RunToCompletionAsync()` starts the host, runs the job, and stops and disposes the host afterwards. Starting it creates the OpenTelemetry providers, and disposing it flushes them, so the run's traces reach your backend before the process exits. SIGTERM cancels the job. The runner waits for the Dapr sidecar, runs the receive pipeline to discover and enqueue source items, subscribes to the pub/sub topic to trigger the send pipeline for each message, and shuts down after an idle timeout. It returns `0` on success (or when the host cancels), `1` on failure or when source files were left in place, and `2` when the sidecar never became available.

## Handle results

When working with pipelines directly (outside the TI lifecycle), results are a discriminated union with four variants:

```csharp
var (result, context) = await sendPipeline.Execute(input, new Context(new Dictionary<string, string>()));

switch (result)
{
    case StepResult<string>.Success success:
        // success.Value contains the sender's output
        break;

    case StepResult<string>.Cancelled:
        // Idempotency check determined this was already processed
        break;

    case StepResult<string>.BusinessFailure bf:
        // bf.Value is a BusinessIncident with Description, OccurredAt, and Context
        break;

    case StepResult<string>.TechnicalFailure tf:
        // tf.Value is a TechnicalFailure with Description, ErrorMessage, and Exception
        break;
}
```

!!! info "Result propagation"
    In a Transactional Integration, you don't handle results directly — the lifecycle
    manages execution and the `BusinessIncidentRouteStep` finalizer routes failures
    automatically.

## Next steps

- [Pipeline Execution](concepts/pipeline-execution.md) — how the pipeline engine chains steps
- [Result Types](concepts/result-types.md) — the discriminated union result system
- [Step Types](concepts/step-types.md) — choosing between BusinessStep, TechnicalStep, and Finalizer
- [Transactional Integration](blocks/transactional-integration.md) — the full receive + send lifecycle
