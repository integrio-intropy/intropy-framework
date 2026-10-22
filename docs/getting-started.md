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

The Hosting package provides `AddTransactionalIntegration` and the `JobRunner` that hosts it, and transitively brings in Blocks, Adapters, and Core. If you only need the pipeline engine without the TI lifecycle, install `Intropy.Framework.Blocks` (or just `Intropy.Framework.Core`) instead.

## Register framework services

In your `Program.cs`, register the core framework and Transactional Integration services:

```csharp
builder.Services.AddIntropyFramework(opts => opts.ComponentName = "order-processor");

builder.Services.AddTransactionalIntegration(opts =>
{
    // The internal queue defaults to the pub/sub and topic the deployment names in
    // INTROPY_PUBSUB_NAME and INTROPY_TOPIC_NAME.
    opts.IdleTimeout = TimeSpan.FromSeconds(5);
});
```

`AddIntropyFramework` registers `FrameworkOptions` and validates the component name. You can also set the `INTROPY_COMPONENT_NAME` environment variable and call `AddIntropyFramework()` without a delegate. The pub/sub the component runs on, `PubSubName`, comes from the deployment through the `INTROPY_PUBSUB_NAME` environment variable: the system topology names its pub/sub components and picks the one each component uses, the system's bus for an extractor or a loader and the internal-queue pub/sub for a transactional integration. The topic it runs on, `TopicName`, comes the same way through `INTROPY_TOPIC_NAME`: a transactional integration's internal-queue topic, or the one topic an extractor or a loader uses. The environment wins over a value set in the delegate; set them in code only for tests or runs without a system host. The framework derives no names of its own.

To export traces and metrics, configure OpenTelemetry with [Intropy.Telemetry](https://github.com/integrio-intropy/intropy-telemetry). Use the component name as its `ServiceName`, and add the framework's meters. See [Observability](concepts/observability.md#collecting-the-telemetry).

`AddTransactionalIntegration` registers the lifecycle as a run-to-completion job, together with the `JobRunner` that hosts it. `DaprPubSubName` and `DaprTopicName` are optional. If you don't set them, they default to the internal queue that the system topology generates for the component: the pub/sub and topic the deployment names, `PubSubName` and `TopicName` (the topology names the topic `internal-<component>`). For a transactional integration that is the system's internal-queue pub/sub, separate from the bus because an internal queue has a single consumer: on Azure Service Bus it deploys as a queue component and the bus as a topic component. Without `PubSubName` and `DaprPubSubName`, or without `TopicName` and `DaprTopicName`, registration fails. Each integration gets its own topic because the broker separates queues by topic, not by Dapr component.

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
// or, to archive handled files: AddSourcePort("order-source", builder.Configuration, FileCompletion.Archive("archive"))
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

`SerializeStep<T, TCtx>` extends `TechnicalStep<T, byte[], TCtx>`. The bytes it returns are what the sender receives and writes as they are, so the serializer owns the encoding:

```csharp
public class InvoiceSerializer : SerializeStep<Invoice, Context>
{
    public override Task<(TechnicalStepResult<byte[]> Result, Context Context)> ExecuteAsync(
        Invoice input, Context context)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(input);
        return Task.FromResult<(TechnicalStepResult<byte[]>, Context)>(
            (new TechnicalStepResult<byte[]>.Success(json), context));
    }
}
```

### Sender

`SendStep<TCtx>` extends `BusinessStep<byte[], byte[], TCtx>`. An exception out of it — the API is
down, the connection drops — raises a business incident *and* leaves the message for redelivery, so
a destination that is merely unavailable never costs you the message:

```csharp
public class InvoiceApiSender(HttpClient httpClient) : SendStep<Context>
{
    public override async Task<(BusinessStepResult<byte[]> Result, Context Context)> ExecuteAsync(
        byte[] input, Context context)
    {
        using var content = new ByteArrayContent(input);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        var response = await httpClient.PostAsync("/invoices", content);

        // The API took the invoice and refused it: the data is wrong, so retrying the same bytes
        // cannot help. Say so, and the message is acknowledged once the incident is routed.
        if (response.StatusCode == HttpStatusCode.UnprocessableEntity)
        {
            var incident = new BusinessIncidentData
            {
                Description = $"The invoice API rejected the invoice: {await response.Content.ReadAsStringAsync()}",
                Context = new Dictionary<string, string>()
            };
            return (new BusinessStepResult<byte[]>.Failure(incident, Retriable: false), context);
        }

        // Anything else throws, which raises an incident and keeps the message for another attempt.
        response.EnsureSuccessStatusCode();

        return (new BusinessStepResult<byte[]>.Success(input), context);
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

The `JobRunner` hosts the lifecycle. Run it through the host, and return its exit code:

```csharp
return await app.RunToCompletionAsync();
```

`RunToCompletionAsync()` starts the host, runs the job, and stops and disposes the host afterwards. Starting it creates the OpenTelemetry providers, and disposing it flushes them, so the run's traces reach your backend before the process exits. SIGTERM cancels the job. The runner waits for the Dapr sidecar, runs the receive pipeline to discover and enqueue source items, subscribes to the pub/sub topic to trigger the send pipeline for each message, and shuts down after an idle timeout. It returns `0` on success (or when the host cancels), `1` on failure, when source files were left in place, or when messages were left for redelivery (returned for retry, or still in flight when the grace period ended), and `2` when the sidecar never became available.

## Handle results

When working with pipelines directly (outside the TI lifecycle), results are a discriminated union with four variants:

```csharp
var (result, context) = await sendPipeline.Execute(input, new Context(new Dictionary<string, string>()));

switch (result)
{
    case StepResult<byte[]>.Success success:
        // success.Value contains the sender's output
        break;

    case StepResult<byte[]>.Cancelled:
        // Idempotency check determined this was already processed
        break;

    case StepResult<byte[]>.BusinessFailure bf:
        // bf.Value is a BusinessIncident with Description, OccurredAt, and Context
        break;

    case StepResult<byte[]>.TechnicalFailure tf:
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
