# Transactional Integration

A framework for building file-based integration flows using Dapr PubSub, designed for Kubernetes CronJobs.

## Overview

This block provides a complete lifecycle for processing files through a message queue. It uses two pipelines:

1. **ReceivePipeline** - Publishes each source file to a queue. Registered by the hosting lifecycle:
   the file sweep (`FileSweep`) reads the file before it and deletes or archives it only after it succeeds
2. **SendPipeline** - Consumes messages from the queue and processes them

## Integration Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Transactional Integration Lifecycle                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  FileSweep (Hosting): list the source, then for each file:                  │
│    read content ─▶ ReceivePipeline ─▶ delete/archive only on success        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        ReceivePipeline                              │    │
│  │    DaprTopicEnqueuer → Publish to message queue (built in)          │    │
│  │                                                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│         │                                                                   │
│         ▼                                                                   │
│  ┌─────────────────┐                                                        │
│  │  Message Queue  │  (Dapr PubSub)                                         │
│  └─────────────────┘                                                        │
│         │                                                                   │
│         ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         SendPipeline                                │    │
│  │  For each message:                                                  │    │
│  │    DeserializeStep     → Convert bytes to POCO                      │    │
│  │    IdempotencyCheck    → Skip if already processed                  │    │
│  │    ExtractStep         → Enrich data (lookups, external calls)      │    │
│  │    ValidateStep        → Business validation                        │    │
│  │    TransformStep       → Convert input POCO to output POCO          │    │
│  │    SerializeStep       → Convert output to string                   │    │
│  │    SendStep            → Send to destination                        │    │
│  │    IdempotencyRecord   → Mark as processed                          │    │
│  │    (BusinessIncidentRouter finalizer)                               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│         │                                                                   │
│         ▼                                                                   │
│  Idle Timeout Reached → Graceful Shutdown → Application Exits               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Define Your Types

```csharp
// Input after deserialization
public class OrderInput
{
    public string OrderId { get; init; }
    public DateTime EventDateTime { get; init; }
}

// Output after transformation
public class OrderOutput
{
    public string OrderId { get; set; }
    public string Status { get; set; }
}

// Custom context (optional, can use Context directly)
public record OrderContext() : Context(new Dictionary<string, string>())
{
    public static OrderContext Create() => new();
}
```

### 2. Implement SendPipeline Steps

```csharp
public class JsonOrderDeserializer : DeserializeStep<OrderInput, Context>
{
    public override Task<(BusinessStepResult<OrderInput> Result, Context Context)>
        ExecuteAsync(ReadOnlyMemory<byte> input, Context context)
    {
        var order = JsonSerializer.Deserialize<OrderInput>(input.Span);
        return Task.FromResult<(BusinessStepResult<OrderInput>, Context)>(
            (new BusinessStepResult<OrderInput>.Success(order!), context));
    }
}

public class OrderValidator : ValidateStep<OrderInput, Context>
{
    public override Task<(BusinessStepResult<OrderInput> Result, Context Context)>
        ExecuteAsync(OrderInput input, Context context)
    {
        if (string.IsNullOrEmpty(input.OrderId))
        {
            var incident = new BusinessIncident(
                "OrderId is required",
                DateTimeOffset.UtcNow,
                new Dictionary<string, string>());
            return Task.FromResult<(BusinessStepResult<OrderInput>, Context)>(
                (new BusinessStepResult<OrderInput>.Failure(incident), context));
        }

        return Task.FromResult<(BusinessStepResult<OrderInput>, Context)>(
            (new BusinessStepResult<OrderInput>.Success(input), context));
    }
}

public class OrderTransformer : TransformStep<OrderInput, OrderOutput, Context>
{
    public override Task<(TechnicalStepResult<OrderOutput> Result, Context Context)>
        ExecuteAsync(OrderInput input, Context context)
    {
        var output = new OrderOutput
        {
            OrderId = input.OrderId,
            Status = "Processed"
        };

        return Task.FromResult<(TechnicalStepResult<OrderOutput>, Context)>(
            (new TechnicalStepResult<OrderOutput>.Success(output), context));
    }
}

public class XmlOrderSerializer : SerializeStep<OrderOutput, Context>
{
    public override async Task<(TechnicalStepResult<string> Result, Context Context)>
        ExecuteAsync(OrderOutput input, Context context)
    {
        var serializer = new System.Xml.Serialization.XmlSerializer(typeof(OrderOutput));
        await using var writer = new StringWriter();
        serializer.Serialize(writer, input);

        return (new TechnicalStepResult<string>.Success(writer.ToString()), context);
    }
}

public class HttpOrderSender : SendStep<Context>
{
    private readonly HttpClient _httpClient;

    public HttpOrderSender(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public override async Task<(BusinessStepResult<string> Result, Context Context)>
        ExecuteAsync(string input, Context context)
    {
        var response = await _httpClient.PostAsync(
            "/api/orders",
            new StringContent(input, Encoding.UTF8, "application/xml"));

        response.EnsureSuccessStatusCode();

        return (new BusinessStepResult<string>.Success(input), context);
    }
}

// Simple pass-through extractor when no enrichment is needed
public class PassThroughExtractor : ExtractStep<OrderInput, Context>
{
    public override Task<(BusinessStepResult<OrderInput> Result, Context Context)>
        ExecuteAsync(OrderInput input, Context context)
    {
        return Task.FromResult<(BusinessStepResult<OrderInput>, Context)>(
            (new BusinessStepResult<OrderInput>.Success(input), context));
    }
}
```

### 3. Configure in Program.cs

```csharp
using Intropy.Libs.Framework.Blocks.TransactionalIntegration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

// Add framework services
builder.Services.AddIntropyFramework();

// Add Dapr client
builder.Services.AddDaprClient();
builder.Services.AddDaprPubSubClient();

// The port the file sweep reads; its adapter is configured under Ports:order-source
builder.Services.AddSourcePort("order-source", builder.Configuration); // or SweepCompletion.Archive("archive")

// Add HTTP client for the sender
builder.Services.AddHttpClient();

// Configure the lifecycle, including the receive side (sweep + publish to the topic)
builder.Services.AddTransactionalIntegration(options =>
{
    options.DaprPubSubName = "order-pubsub";
    options.DaprTopicName = "order-processing";
    options.IdleTimeout = TimeSpan.FromSeconds(5);
    options.PostIdleGracePeriod = TimeSpan.FromSeconds(45);
    options.MaxMessageProcessingTime = TimeSpan.FromSeconds(40);
});

// Register the SendPipeline
builder.Services.AddSendPipeline<OrderInput, OrderOutput, Context>("OrderSendPipeline",
    (builder, sp) => builder
        .WithDeserializer(new JsonOrderDeserializer())
        .WithIdempotency(
            sp.GetRequiredService<IIdempotencyServiceClient>(),
            order => order.OrderId,
            order => order.EventDateTime)
        .WithExtractor(new PassThroughExtractor())
        .WithValidator(new OrderValidator())
        .WithTransformer(new OrderTransformer())
        .WithSerializer(new XmlOrderSerializer())
        .WithSender(new HttpOrderSender(sp.GetRequiredService<HttpClient>()))
        .WithBusinessIncidents(
            sp.GetRequiredService<IBusinessIncidentServiceClient>(),
            ctx => ctx.Metadata.GetValueOrDefault("MessageId", "unknown")));

var host = builder.Build();

// Run the lifecycle to completion (sidecar wait, sweep + subscription, sidecar shutdown)
var runner = host.Services.GetRequiredService<RunToCompletionRunner>();
return await runner.RunAsync();
```

## Configuration Options

### TransactionalIntegrationOptions

| Property | Default | Description |
|----------|---------|-------------|
| `DaprPubSubName` | *required* | Name of the Dapr PubSub component |
| `DaprTopicName` | *required* | Name of the topic to publish/subscribe |
| `IdleTimeout` | 5 seconds | Time to wait after no new messages before shutdown |
| `PostIdleGracePeriod` | 45 seconds | Grace period for in-flight messages to complete |
| `MaxMessageProcessingTime` | 40 seconds | Maximum time allowed for processing a single message |

## ReceivePipeline Steps

The ReceivePipeline runs once per source file, inside the file sweep. `AddTransactionalIntegration`
registers it with one step:

| Step | Class | Description |
|------|------------|-------------|
| **Enqueue** | `DaprTopicEnqueuer<TCtx>` (an `EnqueueStep<TCtx>`) | Publishes the item to `DaprPubSubName` / `DaprTopicName` as a structured CloudEvent carrying `Context.Metadata` and the trace parent |

A failed publish is a technical failure; the sweep then keeps the source file for the next run.
Register your own `EnqueueStep<TCtx>` to replace the publisher (tests register `FakeEnqueueStep`),
or an `IReceivePipeline<TCtx>` (`AddReceivePipeline`) to replace the whole pipeline.

## SendPipeline Steps

The SendPipeline processes messages in this order:

| Step | Base Class | Description |
|------|------------|-------------|
| **Deserialize** | `DeserializeStep<TInput, TCtx>` | Convert bytes to POCO (BusinessStep) |
| **Idempotency Check** | `IdempotencyCheckStep<TInput, TCtx>` | Skip if already processed |
| **Extract** | `ExtractStep<TInput, TCtx>` | Enrich data - lookups, external calls (BusinessStep) |
| **Validate** | `ValidateStep<TInput, TCtx>` | Business validation (BusinessStep) |
| **Transform** | `TransformStep<TInput, TOutput, TCtx>` | Convert input to output (TechnicalStep) |
| **Serialize** | `SerializeStep<TOutput, TCtx>` | Convert output to string (TechnicalStep) |
| **Send** | `SendStep<TCtx>` | Send to destination (BusinessStep) |
| **Idempotency Record** | `IdempotencyRecordStep<string, TCtx>` | Mark as processed |
| **Route Incidents** | Finalizer | Routes any business failures to incident service |

## Source Sweep

The hosting lifecycle sweeps the source with the component-neutral `FileSweep` from
`Intropy.Framework.Hosting` (the same sweep extractors use): it lists the keyed `IFileAdapter`, reads each file, runs the ReceivePipeline in
the file's own scope, and only then completes the file — `Delete` or `Archive(basePath)`. A file
whose publish failed stays in the source and counts as failed, so the run exits 1. Delivery is
at-least-once: a crash after the publish but before completion re-publishes the file on the next
run, which the SendPipeline's idempotency absorbs.

Each file is its own trace, linked to the job's span (as for extractors), and the trace continues
through the queue into the SendPipeline. Each file gets a fresh `Context` whose metadata holds the source file name under
`SourceContextKeys.FileName` (`"file_name"`), the same key extractors get. Context metadata
travels with the message, so the SendPipeline sees it too. The receive pipeline is resolved once
before the source is listed: a missing registration fails the run instead of every file.

To use your own context type in both pipelines, register with
`AddTransactionalIntegration<TCtx>(contextFactory, ...)`. The `ContextFactory<TCtx>` receives the
metadata and retry flag (`(metadata, isRetry) => new OrderContext(metadata, isRetry)`) and is
called for each file and each message.

## Data Types

```csharp
// A source file with its content, ready for publishing
public record SourceItem(string Id, byte[] Data);
```
