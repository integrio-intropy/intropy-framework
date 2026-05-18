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

The Hosting package provides `TransactionalIntegrationRunner` and `AddTransactionalIntegration` and transitively brings in Blocks, Adapters, and Core. If you only need the pipeline engine without the TI lifecycle, install `Intropy.Framework.Blocks` (or just `Intropy.Framework.Core`) instead.

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

`AddTransactionalIntegration` registers the lifecycle services: `TransactionalIntegrationRunner`, message subscriber, and idle timeout monitor. Both `DaprPubSubName` and `DaprTopicName` are required.

## Define your data models

```csharp
public record Order(string OrderId, DateTime CreatedAt, string CustomerName, decimal Amount);

public record Invoice(string InvoiceNumber, string CustomerName, decimal Total, DateTime IssuedDate);
```

## Implement the receive pipeline

The receive pipeline reads source items and publishes them to Dapr pub/sub.

### Source lister

Implement `ISourceLister` to tell the framework what items are available:

```csharp
public class OrderSourceLister(IFileAdapter fileAdapter) : ISourceLister
{
    public async Task<List<SourceItemInfo>> ListSourceItemsAsync()
    {
        var files = await fileAdapter.ListAsync();
        return files.Select(f => new SourceItemInfo(f.FileName)).ToList();
    }
}
```

### Receive step

`ReceiveStep<TCtx>` extends `BusinessStep<SourceItemInfo, SourceItem, TCtx>`. It reads the content for a source item:

```csharp
public class OrderReceiver(IFileAdapter fileAdapter) : ReceiveStep<Context>
{
    public override async Task<(BusinessStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
        SourceItemInfo input, Context context)
    {
        var content = await fileAdapter.GetContentAsync(input.Id);
        var sourceItem = new SourceItem(input.Id, content);
        return (new BusinessStepResult<SourceItem>.Success(sourceItem), context);
    }
}
```

### Enqueue step

`EnqueueStep<TCtx>` extends `TechnicalStep<SourceItem, SourceItem, TCtx>`. Its `ExecuteAsync` is sealed — it propagates context metadata and trace information automatically. You implement the overload that receives a `messageMetadata` dictionary:

```csharp
public class OrderEnqueuer(DaprClient daprClient) : EnqueueStep<Context>
{
    public override async Task<(TechnicalStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
        SourceItem input, Context context, Dictionary<string, string> messageMetadata)
    {
        await daprClient.PublishByteEventAsync(
            "pubsub", "orders", input.Data, messageMetadata);

        return (new TechnicalStepResult<SourceItem>.Success(input), context);
    }
}
```

### Complete step

`CompleteStep<TCtx>` extends `BusinessStep<SourceItem, SourceItem, TCtx>`. It handles cleanup after successful enqueue (e.g., deleting or archiving the source file):

```csharp
public class OrderCompleter(IFileAdapter fileAdapter) : CompleteStep<Context>
{
    public override async Task<(BusinessStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
        SourceItem input, Context context)
    {
        await fileAdapter.DeleteAsync(input.Id);
        return (new BusinessStepResult<SourceItem>.Success(input), context);
    }
}
```

### Register the receive pipeline

```csharp
builder.Services.AddSingleton<ISourceLister, OrderSourceLister>();

builder.Services.AddReceivePipeline<Context>("order-receive",
    (pipelineBuilder, sp) => pipelineBuilder
        .WithReceiver(sp.GetRequiredService<OrderReceiver>())
        .WithEnqueuer(sp.GetRequiredService<OrderEnqueuer>())
        .WithCompleter(sp.GetRequiredService<OrderCompleter>())
        .WithBusinessIncidents(
            sp.GetRequiredService<IBusinessIncidentServiceClient>(),
            ctx => ctx.Metadata["message_id"]));
```

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

The `TransactionalIntegrationRunner` orchestrates the lifecycle. Resolve and run it:

```csharp
var runner = app.Services.GetRequiredService<TransactionalIntegrationRunner>();
var exitCode = await runner.RunAsync();
return exitCode;
```

`RunAsync()` waits for the Dapr sidecar, runs the receive pipeline to discover and enqueue source items, subscribes to the pub/sub topic to trigger the send pipeline for each message, and shuts down after an idle timeout. It returns `0` on success and `1` on failure.

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
