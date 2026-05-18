# Transactional Integration

A framework for building file-based integration flows using Dapr PubSub, designed for Kubernetes CronJobs.

## Overview

This block provides a complete lifecycle for processing files through a message queue. It uses two pipelines:

1. **ReceivePipeline** - Reads files from storage and publishes them to a queue
2. **SendPipeline** - Consumes messages from the queue and processes them

## Integration Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Transactional Integration Lifecycle                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ISourceLister.ListItemsAsync()                                             │
│         │                                                                   │
│         ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        ReceivePipeline                              │    │
│  │  For each item:                                                     │    │
│  │    ReceiveStep    → Read content from source (file adapter)         │    │
│  │    EnqueueStep    → Publish to message queue                        │    │
│  │    CompleteStep   → Delete/archive source file                      │    │
│  │    (BusinessIncidentRouter finalizer)                               │    │
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

### 2. Implement ReceivePipeline Steps

```csharp
// Reads file content using a file adapter
public class FileReceiver : ReceiveStep<Context>
{
    private readonly IFileAdapter _fileAdapter;

    public FileReceiver(IFileAdapter fileAdapter)
    {
        _fileAdapter = fileAdapter;
    }

    public override async Task<(BusinessStepResult<SourceItem> Result, Context Context)>
        ExecuteAsync(SourceItemInfo input, Context context)
    {
        var content = await _fileAdapter.GetContentAsync(input.Id);
        var sourceItem = new SourceItem(input.Id, content);
        return (new BusinessStepResult<SourceItem>.Success(sourceItem), context);
    }
}

// Publishes to message queue using Dapr
public class DaprEnqueuer : EnqueueStep<Context>
{
    private readonly IMessagePublisher _messagePublisher;

    public DaprEnqueuer(IMessagePublisher messagePublisher)
    {
        _messagePublisher = messagePublisher;
    }

    public override async Task<(TechnicalStepResult<SourceItem> Result, Context Context)>
        ExecuteAsync(SourceItem input, Context context, Dictionary<string, string> messageContext)
    {
        await _messagePublisher.PublishAsync(input.Data, messageContext);
        return (new TechnicalStepResult<SourceItem>.Success(input), context);
    }
}

// Deletes the source file after successful publish
public class DeleteCompleter : CompleteStep<Context>
{
    private readonly IFileAdapter _fileAdapter;

    public DeleteCompleter(IFileAdapter fileAdapter)
    {
        _fileAdapter = fileAdapter;
    }

    public override async Task<(BusinessStepResult<SourceItem> Result, Context Context)>
        ExecuteAsync(SourceItem input, Context context)
    {
        await _fileAdapter.DeleteAsync(input.Id);
        return (new BusinessStepResult<SourceItem>.Success(input), context);
    }
}
```

### 3. Implement SendPipeline Steps

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

### 4. Configure in Program.cs

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

// Add your file adapter implementation
builder.Services.AddSingleton<IFileAdapter, SftpAdapter>();

// Add source lister (uses file adapter to list files)
builder.Services.AddSingleton<ISourceLister, FileSourceLister>();

// Add message publisher (uses Dapr)
builder.Services.AddSingleton<IMessagePublisher, DaprMessagePublisher>();

// Add HTTP client for the sender
builder.Services.AddHttpClient();

// Configure the lifecycle
builder.Services.AddTransactionalIntegration(options =>
{
    options.DaprPubSubName = "order-pubsub";
    options.DaprTopicName = "order-processing";
    options.IdleTimeoutSeconds = 5;
    options.PostIdleGracePeriodSeconds = 45;
    options.MaxMessageProcessingTimeSeconds = 40;
});

// Register the ReceivePipeline
builder.Services.AddReceivePipeline("OrderReceivePipeline",
    (builder, sp) => builder
        .WithReceiver(new FileReceiver(sp.GetRequiredService<IFileAdapter>()))
        .WithEnqueuer(new DaprEnqueuer(sp.GetRequiredService<IMessagePublisher>()))
        .WithCompleter(new DeleteCompleter(sp.GetRequiredService<IFileAdapter>()))
        .WithBusinessIncidents(
            sp.GetRequiredService<IBusinessIncidentServiceClient>(),
            ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown")));

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

// Start the lifecycle
var lifecycle = host.Services.GetRequiredService<TransactionalIntegrationLifecycle>();
await lifecycle.Start();
```

## Configuration Options

### TransactionalIntegrationOptions

| Property | Default | Description |
|----------|---------|-------------|
| `DaprPubSubName` | *required* | Name of the Dapr PubSub component |
| `DaprTopicName` | *required* | Name of the topic to publish/subscribe |
| `IdleTimeoutSeconds` | 5 | Time to wait after no new messages before shutdown |
| `PostIdleGracePeriodSeconds` | 45 | Grace period for in-flight messages to complete |
| `MaxMessageProcessingTimeSeconds` | 40 | Maximum time allowed for processing a single message |

## ReceivePipeline Steps

The ReceivePipeline processes source items in this order:

| Step | Base Class | Description |
|------|------------|-------------|
| **Receive** | `ReceiveStep<TCtx>` | Reads content from the source (BusinessStep) |
| **Enqueue** | `EnqueueStep<TCtx>` | Publishes to message queue (TechnicalStep) |
| **Complete** | `CompleteStep<TCtx>` | Cleanup after publish - delete/archive (BusinessStep) |
| **Route Incidents** | Finalizer | Routes any business failures to incident service |

### Step Types
- **ReceiveStep**: Returns `BusinessStepResult<SourceItem>`. Failures indicate issues with the source (file not found, access denied, corrupt data).
- **EnqueueStep**: Returns `TechnicalStepResult<SourceItem>`. Failures indicate queue issues (unavailable, publish failed).
- **CompleteStep**: Returns `BusinessStepResult<SourceItem>`. Failures indicate cleanup issues (can't delete, can't archive).

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

## Source Listing

The `ISourceLister` interface is responsible for listing items to process. It runs outside the pipeline:

```csharp
public interface ISourceLister
{
    Task<IReadOnlyList<SourceItemInfo>> ListItemsAsync(CancellationToken cancellationToken = default);
}
```

A default `FileSourceLister` implementation is provided that uses `IFileAdapter.ListAsync()`.

## Data Types

```csharp
// Metadata about an item to be processed (returned by listing operation)
public record SourceItemInfo(string Id);

// An item with its content, ready for publishing
public record SourceItem(string Id, byte[] Data);
```
