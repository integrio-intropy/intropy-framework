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

Configures serialization from `TOutput` to `byte[]`. **Required.** The bytes are sent as they are, so the serializer owns the encoding.

### WithSender

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithSender(SendStep<TCtx> sender)
```

Configures sending (`byte[]` to `byte[]`). **Required.**

### WithIdempotency

```csharp
public SendPipelineBuilder<TInput, TOutput, TCtx> WithIdempotency(
    IIdempotencyServiceClient client,
    Func<TInput, string> idExtractor,
    Func<TInput, DateTime> dateExtractor,
    Func<TInput, string>? hashGenerator = null)
```

Configures automatic idempotency using the external idempotency service. Sets up both the check step (before validation) and the record step (after sending). **Optional**: without it (or `WithCustomIdempotency`), both steps are skipped.

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
    IdempotencyRecordStep<byte[], TCtx> idempotencyRecorder)
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
    BusinessIncidentRouteStep<byte[], TCtx> businessIncidentRouteStep)
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

### WithEnqueuer

```csharp
public ReceivePipelineBuilder<TCtx> WithEnqueuer(EnqueueStep<TCtx> enqueuer)
```

Configures the enqueue step that publishes content to the Dapr topic. **Required.**

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

Registers `IReceivePipeline<TContext>` as a singleton. `AddTransactionalIntegration` already registers a receive pipeline that publishes with `DaprTopicEnqueuer<TContext>`; use this only to replace it.

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

Configures deserialization from the raw bytes (`ReadOnlyMemory<byte>`) to `TInput`. **Required.** The bytes are not decoded beforehand, so the step owns the encoding.

!!! note "Different from SendPipeline"
    The Extractor's `DeserializeStep` and the Send pipeline's both take `ReadOnlyMemory<byte>`, but they are different types in different namespaces.

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

Configures sending CloudEvents to a Dapr service via service invocation. Invokes the target service's `"ingest"` endpoint. A non-success status code is a technical failure.

| Parameter | Type | Description |
|-----------|------|-------------|
| `appId` | `string` | Dapr app ID of the target service |
| `source` | `Uri` | CloudEvent source URI |
| `type` | `string` | CloudEvent type |

### WithIdempotency

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithIdempotency(
    Func<TInput, TCtx, string> idExtractor,
    Func<TInput, TCtx, DateTimeOffset> dateExtractor,
    Func<TInput, string>? hashGenerator = null,
    IdempotencyScope scope = IdempotencyScope.Route)
```

Configures idempotency. Resolves `IIdempotencyServiceClient` from the service provider. **Optional**: without it, the check and record steps are skipped and no `IIdempotencyServiceClient` needs to be registered.

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
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan PostIdleGracePeriod { get; set; } = TimeSpan.FromSeconds(45);
    public TimeSpan MaxMessageProcessingTime { get; set; } = TimeSpan.FromSeconds(40);
}
```

### DI Registration

```csharp
services.AddTransactionalIntegration(opts =>
{
    opts.DaprPubSubName = "orders-bus";   // default: INTROPY_PUBSUB_NAME
    opts.DaprTopicName = "orders";        // default: INTROPY_TOPIC_NAME
});
```

Registers `TransactionalIntegrationOptions`, `ITopicSubscriber`, and `TransactionalIntegrationJob` as the `IJob` hosted by `JobRunner`. An optional second delegate configures the runner's `JobOptions` (job name, sidecar timeouts).

## See also

- [Transactional Integration](../blocks/transactional-integration.md) — lifecycle and architecture
- [Steps](steps.md) — step base classes
- [Getting Started](../getting-started.md) — complete example
