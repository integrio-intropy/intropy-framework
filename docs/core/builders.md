# Builders

> Fluent builder APIs for configuring Block pipelines. Signatures describe this source revision.

All builders require `TCtx : Context`. Their method call order configures slots; the block implementation fixes execution order. See the block pages for the sequence. TI differs from Extractor/Loader: its idempotency and business incident routing are optional.

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
    Func<TInput, TCtx, string> idExtractor,
    Func<TInput, TCtx, DateTimeOffset> dateExtractor,
    Func<TInput, string>? hashGenerator = null)
```

Configures automatic idempotency using the external idempotency service. Sets up both the check step (before validation) and the record step (after sending). **Optional**; omit both check and record, or use `WithCustomIdempotency` instead.

| Parameter | Type | Description |
|-----------|------|-------------|
| `client` | `IIdempotencyServiceClient` | Client for the idempotency service |
| `idExtractor` | `Func<TInput, TCtx, string>` | Extracts the unique ID from the deserialized input |
| `dateExtractor` | `Func<TInput, TCtx, DateTimeOffset>` | Extracts the event date from the deserialized input |
| `hashGenerator` | `Func<TInput, string>?` | Optional custom hash function. Default: JSON + SHA256 + Base64, or `IHashable`; see [hash rules](../blocks/loader.md#hash-generation) |

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
    Func<TCtx, string> messageIdExtractor,
    Func<TCtx, string> subjectExtractor)
```

Configures automatic business incident routing. On business failure, routes the incident to the service. On success with retry, resolves previous incidents. **Optional**; omit routing, or use `WithCustomBusinessIncidents` instead.

| Parameter | Type | Description |
|-----------|------|-------------|
| `client` | `IBusinessIncidentServiceClient` | Client for the business incident service |
| `messageIdExtractor` | `Func<TCtx, string>` | Extracts a message ID from context, consistent across retries |
| `subjectExtractor` | `Func<TCtx, string>` | Extracts a business-facing subject ID, available even on failure |

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

Builds the pipeline. Throws `InvalidOperationException` if any of the five required steps (deserialize, validate, transform, serialize, send) is missing. Extractors, idempotency, and business incident routing are optional.

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
    Func<TCtx, string> messageIdExtractor,
    Func<TCtx, string> subjectExtractor)
```

Configures automatic business incident routing. **Optional**; omit routing, or use `WithCustomBusinessIncidents` instead.

### WithCustomBusinessIncidents

```csharp
public ReceivePipelineBuilder<TCtx> WithCustomBusinessIncidents(
    BusinessIncidentRouteStep<SourceItem, TCtx> businessIncidentRouteStep)
```

Uses a custom business incident router. It is optional for TI receive pipelines. On receive, the host initially supplies `sourceItemId`, not `message_id`; choose extractors that work before deserialization or enqueue.

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

Registers `IReceivePipeline<TContext>` as a singleton. Configuration and `Build()` run when the service is first resolved; receiver, enqueuer, and completer are the three required steps. The host resolves pipelines using `Context`, not a custom context type.

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

> [!NOTE]
> **Different from SendPipeline**
> The Extractor's `DeserializeStep` takes `string` input, while the Send pipeline's takes `ReadOnlyMemory<byte>`. They are different types in different namespaces.

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
    string type,
    Func<IServiceProvider, HttpClient> httpClientFactory)
```

Configures sending CloudEvents to a Dapr service via service invocation. Builds a POST to the target service's `"ingest"` endpoint and sends it using the supplied HTTP client. The caller owns that client's lifetime. The current implementation does not call `EnsureSuccessStatusCode`; HTTP error statuses alone are not converted to failures. See [service invoker](../blocks/extractor.md#dapr-service-invoker).

| Parameter | Type | Description |
|-----------|------|-------------|
| `appId` | `string` | Dapr app ID of the target service |
| `httpClientFactory` | `Func<IServiceProvider, HttpClient>` | Returns a caller-managed HTTP client |
| `source` | `Uri` | CloudEvent source URI |
| `type` | `string` | CloudEvent type |

### WithIdempotency

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithIdempotency(
    Func<TInput, TCtx, string> idExtractor,
    Func<TInput, TCtx, DateTimeOffset> dateExtractor,
    Func<TInput, string>? hashGenerator = null)
```

Configures idempotency. Resolves `IIdempotencyServiceClient` from the service provider. **Required.**

### WithBusinessIncidents

```csharp
public ExtractorBuilder<TInput, TOutput, TCtx> WithBusinessIncidents(
    Func<TCtx, string> messageIdExtractor,
    Func<TCtx, string> subjectExtractor)
```

Configures business incident routing. Resolves `IBusinessIncidentServiceClient` from the service provider. **Required.**

### Build

```csharp
public Extractor<TInput, TOutput, TCtx> Build()
```

---

## LoaderBuilder<TInput, TOutput, TCtx>

**Namespace:** `Intropy.Framework.Blocks.Loader`

`Create(string pipelineName, IServiceProvider serviceProvider)` resolves `FrameworkOptions` and `ILoggerFactory`, like Extractor. Required slots: deserializer, validator, transformer, sender, idempotency, and business incident router. Extractors and the receipt sender are optional.

| Method | Argument / behavior |
|---|---|
| `WithDeserializer` | Loader `DeserializeStep<TInput, TCtx>`; implement protected `DeserializeAsync` |
| `WithExtractor` | Loader `ExtractStep<TInput, TCtx>`; repeatable |
| `WithValidator` | Loader `ValidateStep<TInput, TCtx>` |
| `WithTransformer` | Loader `TransformStep<TInput, TOutput, TCtx>` |
| `WithSender` | Loader `SendStep<TOutput, TCtx>` |
| `WithReceiptSender` | Loader `SendStep<TOutput, TCtx>`; runs after the idempotency record |
| `WithIdempotency` | Optional `Func<TInput, string>? hashGenerator = null`; reads ID/date from CloudEvent context and resolves `IIdempotencyServiceClient` |
| `WithBusinessIncidents` | `Func<TCtx, string> messageIdExtractor`, `Func<TCtx, string> subjectExtractor`; resolves `IBusinessIncidentServiceClient` |
| `Build` | Returns `Loader<TInput, TOutput, TCtx>`; throws for missing required slots |

All `With...` methods return this builder. There is no separate Loader serializer slot or TI-style custom-idempotency/custom-incident builder method. See [Loader](../blocks/loader.md) for composition and metadata rules.

## TransactionalIntegrationOptions

These options belong to **Hosting**, in `Intropy.Framework.Hosting.TransactionalIntegration.Job`, not Blocks. Their canonical reference is [TI configuration](../blocks/transactional-integration.md#configuration).

### DI Registration

`AddTransactionalIntegration` is also in Hosting. It registers options, `ITopicSubscriber`, the lifecycle, and runner; it does not register your pipelines, source lister, or Dapr clients. See [Getting Started](../getting-started.md#register-framework-services) for complete wiring.

## See also

- [Transactional Integration](../blocks/transactional-integration.md) — lifecycle and architecture
- [Steps](steps.md) — step base classes
- [Getting Started](../getting-started.md) — complete example

Source: [SendPipelineBuilder.cs](../../src/Intropy.Framework.Blocks/TransactionalIntegration/Send/SendPipelineBuilder.cs), [ReceivePipelineBuilder.cs](../../src/Intropy.Framework.Blocks/TransactionalIntegration/Receive/ReceivePipelineBuilder.cs), [ExtractorBuilder.cs](../../src/Intropy.Framework.Blocks/Extractor/ExtractorBuilder.cs), [LoaderBuilder.cs](../../src/Intropy.Framework.Blocks/Loader/LoaderBuilder.cs).
