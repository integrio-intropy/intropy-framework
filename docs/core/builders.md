# Builders

> Fluent builder APIs for configuring Block pipelines.

## SendPipelineBuilder\<TInput, TOutput, TCtx\>

**Namespace:** `Intropy.Framework.Blocks.TransactionalIntegration.Send`
**Assembly:** `Intropy.Framework.Blocks`
**Constraint:** `TCtx : Context`

Configures a send pipeline for Transactional Integration.

### Create

```csharp
public static SendPipelineBuilder<TInput, TOutput, TCtx> Create(
    string pipelineName,
    FrameworkOptions frameworkOptions,
    ILoggerFactory loggerFactory)
```

Creates a new builder instance. Called automatically by `AddSendPipeline`.

### WithDeserializer

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithDeserializer(DeserializeStep<TInput, TCtx> deserializer)
```

Configures deserialization from `ReadOnlyMemory<byte>` to `TInput`. **Required.**

### WithExtractor

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithExtractor(ExtractStep<TInput, TCtx> extractor)
```

Adds an extract step. Can be called multiple times — extractors execute in order. **Optional.**

### WithValidator

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithValidator(ValidateStep<TInput, TCtx> validator)
```

Configures input validation. **Required.**

### WithTransformer

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithTransformer(TransformStep<TInput, TOutput, TCtx> transformer)
```

Configures transformation from `TInput` to `TOutput`. **Required.**

### WithSerializer

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithSerializer(SerializeStep<TOutput, TCtx> serializer)
```

Configures serialization from `TOutput` to `string`. **Required.**

### WithSender

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithSender(SendStep<TCtx> sender)
```

Configures sending (`string` to `string`). **Required.**

### WithIdempotency

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithIdempotency(
    IIdempotencyServiceClient client,
    Func<TInput, string> idExtractor,
    Func<TInput, DateTime> dateExtractor,
    Func<TInput, string>? hashGenerator = null)
```

Configures automatic idempotency using the external idempotency service. Sets up both the check step (before validation) and the record step (after sending). **Required** (or use `WithCustomIdempotency`).

| Parameter | Type | Description |
|-----------|------|-------------|
| `client` | `IIdempotencyServiceClient` | Client for the idempotency service |
| `idExtractor` | `Func<TInput, string>` | Extracts the unique ID from the deserialized input |
| `dateExtractor` | `Func<TInput, DateTime>` | Extracts the event date from the deserialized input |
| `hashGenerator` | `Func<TInput, string>?` | Optional custom hash function. Default: JSON + SHA256 |

### WithCustomIdempotency

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithCustomIdempotency(
    IdempotencyCheckStep<TInput, TCtx> idempotencyCheckStep,
    IdempotencyRecordStep<string, TCtx> idempotencyRecorder)
```

Uses custom idempotency implementations instead of the built-in external service.

### WithBusinessIncidents

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithBusinessIncidents(
    IBusinessIncidentServiceClient client,
    Func<TCtx, string> messageIdExtractor)
```

Configures automatic business incident routing. On business failure, routes the incident to the service. On success with retry, resolves previous incidents. **Required** (or use `WithCustomBusinessIncidents`).

| Parameter | Type | Description |
|-----------|------|-------------|
| `client` | `IBusinessIncidentServiceClient` | Client for the business incident service |
| `messageIdExtractor` | `Func<TCtx, string>` | Extracts a message ID from context, consistent across retries |

### WithCustomBusinessIncidents

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithCustomBusinessIncidents(
    BusinessIncidentRouteStep<string, TCtx> businessIncidentRouteStep)
```

Uses a custom business incident router instead of the built-in external service.

### Build

```csharp
public SendPipeline<TInput, TOutput, TCtx> Build()
```

Builds the pipeline. Throws `InvalidOperationException` if any required step is missing.

### DI Registration

```csharp
services.AddSendPipeline<TInput, TOutput, TContext>(
    string pipelineName,
    Func<SendPipelineBuilder<TInput, TOutput, TContext>, IServiceProvider,
        SendPipelineBuilder<TInput, TOutput, TContext>> configurePipeline)
```

Registers `ISendPipeline<TContext>` as a singleton. The builder is created, configured via the callback, and built during service resolution.

---

## ReceivePipelineBuilder\<TCtx\>

**Namespace:** `Intropy.Framework.Blocks.TransactionalIntegration.Receive`
**Constraint:** `TCtx : Context`

Configures a receive pipeline for Transactional Integration.

### Create

```csharp
public static ReceivePipelineBuilder<TCtx> Create(
    string pipelineName,
    FrameworkOptions frameworkOptions,
    ILoggerFactory loggerFactory)
```

### WithReceiver

```csharp
public ReceivePipelineBuilder<TCtx> WithReceiver(ReceiveStep<TCtx> receiver)
```

Configures the receive step that reads content from the source. **Required.**

### WithEnqueuer

```csharp
public ReceivePipelineBuilder<TCtx> WithEnqueuer(EnqueueStep<TCtx> enqueuer)
```

Configures the enqueue step that publishes content to the Dapr topic. **Required.**

### WithCompleter

```csharp
public ReceivePipelineBuilder<TCtx> WithCompleter(CompleteStep<TCtx> completer)
```

Configures the complete step for cleanup after enqueue. **Required.**

### WithBusinessIncidents

```csharp
public ReceivePipelineBuilder<TCtx> WithBusinessIncidents(
    IBusinessIncidentServiceClient client,
    Func<TCtx, string> messageIdExtractor)
```

Configures automatic business incident routing. **Required** (or use `WithCustomBusinessIncidents`).

### WithCustomBusinessIncidents

```csharp
public ReceivePipelineBuilder<TCtx> WithCustomBusinessIncidents(
    BusinessIncidentRouteStep<SourceItem, TCtx> businessIncidentRouteStep)
```

Uses a custom business incident router.

### Build

```csharp
public ReceivePipeline<TCtx> Build()
```

### DI Registration

```csharp
services.AddReceivePipeline<TContext>(
    string pipelineName,
    Func<ReceivePipelineBuilder<TContext>, IServiceProvider,
        ReceivePipelineBuilder<TContext>> configurePipeline)
```

Registers `IReceivePipeline<TContext>` as a singleton.

---

## ExtractorBuilder\<TInput, TOutput, TCtx\>

**Namespace:** `Intropy.Framework.Blocks.Extractor`
**Constraint:** `TCtx : Context`

Configures an Extractor pipeline that outputs CloudEvents.

### Create

```csharp
public static ExtractorBuilder<TInput, TOutput, TCtx> Create(
    string pipelineName,
    IServiceProvider serviceProvider)
```

Creates a new builder. Resolves `FrameworkOptions` and `ILoggerFactory` from the service provider.

### WithDeserializer

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithDeserializer(DeserializeStep<TInput, TCtx> deserializer)
```

Configures deserialization from `string` to `TInput`. **Required.**

!!! note "Different from SendPipeline"
    The Extractor's `DeserializeStep` takes `string` input, while the Send pipeline's takes `ReadOnlyMemory<byte>`. They are different types in different namespaces.

### WithValidator

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithValidator(ValidateStep<TInput, TCtx> validator)
```

**Required.**

### WithExtractor

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithExtractor(ExtractStep<TInput, TCtx> extractor)
```

Adds an extract step. Can be called multiple times. **Optional.**

### WithTransformer

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithTransformer(TransformStep<TInput, TOutput, TCtx> transformer)
```

**Required.**

### WithSerializer

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithSerializer(SerializeStep<TOutput, TCtx> serializer)
```

Configures serialization from `TOutput` to `CloudEvent`. **Required.**

### WithSender

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithSender(SendStep<TCtx> sender)
```

Configures a custom sender for CloudEvents. Use this or one of the built-in senders below. **Required.**

### WithDaprTopicPublisher

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithDaprTopicPublisher(
    string pubSubName,
    string topicName,
    Uri source,
    string type)
```

Configures sending CloudEvents to a Dapr pub/sub topic. Resolves `DaprClient` from the service provider.

| Parameter | Type | Description |
|-----------|------|-------------|
| `pubSubName` | `string` | Dapr pub/sub component name (e.g., `"pubsub"`) |
| `topicName` | `string` | Topic to publish to (e.g., `"customers"`) |
| `source` | `Uri` | CloudEvent source URI (e.g., `new Uri("urn:company:system:salesforce")`) |
| `type` | `string` | CloudEvent type (e.g., `"com.company.customer.extracted"`) |

### WithDaprServiceInvoker

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithDaprServiceInvoker(
    string appId,
    Uri source,
    string type)
```

Configures sending CloudEvents to a Dapr service via service invocation. Invokes the target service's `"ingest"` endpoint.

| Parameter | Type | Description |
|-----------|------|-------------|
| `appId` | `string` | Dapr app ID of the target service |
| `source` | `Uri` | CloudEvent source URI |
| `type` | `string` | CloudEvent type |

### WithIdempotency

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithIdempotency(
    Func<TInput, string> idExtractor,
    Func<TInput, DateTime> dateExtractor,
    Func<TInput, string>? hashGenerator = null)
```

Configures idempotency. Resolves `IIdempotencyServiceClient` from the service provider. **Required.**

### WithBusinessIncidents

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithBusinessIncidents(
    Func<TCtx, string> messageIdExtractor)
```

Configures business incident routing. Resolves `IBusinessIncidentServiceClient` from the service provider. **Required.**

### Build

```csharp
public Extractor<TInput, TOutput, TCtx> Build()
```

---

## TransactionalIntegrationOptions

**Namespace:** `Intropy.Framework.Blocks.TransactionalIntegration`

```csharp
public class TransactionalIntegrationOptions
{
    public string DaprPubSubName { get; set; }
    public string DaprTopicName { get; set; }
    public int IdleTimeoutSeconds { get; set; } = 5;
    public int PostIdleGracePeriodSeconds { get; set; } = 45;
    public int MaxMessageProcessingTimeSeconds { get; set; } = 40;
    public int SidecarTimeoutSeconds { get; set; } = 30;
}
```

### DI Registration

```csharp
services.AddTransactionalIntegration(opts =>
{
    opts.DaprPubSubName = "pubsub";   // required
    opts.DaprTopicName = "orders";    // required
});
```

Registers `TransactionalIntegrationOptions`, `ITopicSubscriber`, `TransactionalIntegrationLifecycle`, and `TransactionalIntegrationRunner`.

## See also

- [Transactional Integration](../blocks/transactional-integration.md) — lifecycle and architecture
- [Steps](steps.md) — step base classes
- [Getting Started](../getting-started.md) — complete example
