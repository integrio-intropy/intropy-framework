# Loader

> A standalone pipeline that receives a CloudEvent, transforms its data, and loads it into an external system.

## How it works

```mermaid
graph LR
    A[CloudEvent] --> B[Deserialize]
    B --> C["Extract(s)"]
    C --> D[Validate]
    D --> E[Idempotency Check]
    E --> F[Transform]
    F --> G[Send]
    G --> H[Idempotency Record]
    H --> I["Send Receipt (optional)"]
    I --> J[Business Incident Route]
```


The Loader is a standalone pipeline for loading canonical data into a target system. It takes a `CloudEvent` as input, deserializes the payload, optionally enriches it via extract steps, validates it, transforms it to the target model, and sends it to the external system. The output is a `StepResult<TOutput>`.

You call the Loader's `Execute` method directly with a CloudEvent — there is no lifecycle manager or message subscription involved. The Loader handles its own OpenTelemetry tracing (each execution creates a detached trace), idempotency, and business incident routing.

The Loader is the natural counterpart to the Extractor: where the Extractor reads from a source and publishes CloudEvents, the Loader subscribes to CloudEvents and writes to a target. Together they form the two halves of a Golden Record pipeline.

!!! note "Step namespace"
    The Loader has its own step abstractions in
    `Intropy.Framework.Blocks.Loader.Steps`. Make sure you import from
    this namespace — other blocks have separate step types with the same names.

## Pipeline flow

The pipeline executes in this order:

1. **Deserialize** (`CloudEvent → TInput`) — Extract CloudEvent metadata to context, then deserialize the payload into a typed object
2. **Extract** (`TInput → TInput`) — Enrich with additional data (optional, multiple allowed)
3. **Validate** (`TInput → TInput`) — Validate the enriched object
4. **Idempotency Check** (`TInput → TInput`) — Check if this CloudEvent was already processed (uses `Subject` and `Time` from the CloudEvent)
5. **Transform** (`TInput → TOutput`) — Convert to the target system's model
6. **Send** (`TOutput → TOutput`) — Send the data to the external system
7. **Idempotency Record** — Record that this CloudEvent was processed (finalizer)
8. **Send Receipt** (`TOutput → TOutput`) — Send a receipt after a successful load (optional step)
9. **Business Incident Route** — Route any business incidents (finalizer)

Each pipeline execution creates a detached OpenTelemetry trace (a new root span linked to the parent) via `PipelineTracing.ExecuteWithTracing`.

## CloudEvent metadata extraction

The `DeserializeStep` automatically extracts CloudEvent metadata into the context before your deserialization logic runs. These values are available in all subsequent steps via `context.Metadata`:

| Context key | CloudEvent property | Example value |
|-------------|-------------------|---------------|
| `cloudevent.id` | `Id` | `"abc-123"` |
| `cloudevent.subject` | `Subject` | `"CUST-001"` |
| `cloudevent.time` | `Time` | `"2025-01-15T10:30:00.0000000Z"` |
| `cloudevent.source` | `Source` | `"urn:company:system:crm"` |
| `cloudevent.type` | `Type` | `"com.company.customer.extracted"` |

The idempotency checker uses `cloudevent.subject` as the ID and `cloudevent.time` as the date, so both `Subject` and `Time` must be set on the incoming CloudEvent.

## Implementing steps

### DeserializeStep

Extends `BusinessStep<CloudEvent, TInput, TCtx>`. The base class `ExecuteAsync` is **sealed** — it extracts CloudEvent metadata to context automatically, then calls your `DeserializeAsync` override:

```csharp
using Intropy.Framework.Blocks.Loader.Steps;

public class CustomerDeserializer : DeserializeStep<CustomerRecord, Context>
{
    protected override Task<(BusinessStepResult<CustomerRecord> Result, Context Context)> DeserializeAsync(
        CloudEvent cloudEvent, Context context)
    {
        var json = cloudEvent.Data?.ToString();
        var record = JsonSerializer.Deserialize<CustomerRecord>(json!)!;
        return Task.FromResult<(BusinessStepResult<CustomerRecord>, Context)>(
            (new BusinessStepResult<CustomerRecord>.Success(record), context));
    }
}
```

!!! warning "Override DeserializeAsync, not ExecuteAsync"
    `ExecuteAsync` is sealed on the Loader's `DeserializeStep`. Override `DeserializeAsync`
    instead — this ensures CloudEvent metadata is always extracted before your code runs.

### ExtractStep (optional)

Extends `BusinessStep<TInput, TInput, TCtx>`. Enriches the data with information from external sources. You can add multiple extract steps — they run in the order they are registered, before validation:

```csharp
public class EnrichWithAddress : ExtractStep<CustomerRecord, Context>
{
    private readonly IAddressService _addressService;

    public EnrichWithAddress(IAddressService addressService) => _addressService = addressService;

    public override async Task<(BusinessStepResult<CustomerRecord> Result, Context Context)> ExecuteAsync(
        CustomerRecord input, Context context)
    {
        var address = await _addressService.LookupAsync(input.Id);
        var enriched = input with { Address = address };
        return (new BusinessStepResult<CustomerRecord>.Success(enriched), context);
    }
}
```

### ValidateStep

Extends `BusinessStep<T, T, TCtx>`. Same type in and out. Runs after extract steps, so you can validate enriched data:

```csharp
public class CustomerValidator : ValidateStep<CustomerRecord, Context>
{
    public override Task<(BusinessStepResult<CustomerRecord> Result, Context Context)> ExecuteAsync(
        CustomerRecord input, Context context)
    {
        if (string.IsNullOrEmpty(input.Name))
        {
            var incident = new BusinessIncident(
                "Customer name is required",
                DateTimeOffset.UtcNow,
                new Dictionary<string, string> { ["customerId"] = input.Id });
            return Task.FromResult<(BusinessStepResult<CustomerRecord>, Context)>(
                (new BusinessStepResult<CustomerRecord>.Failure(incident), context));
        }

        return Task.FromResult<(BusinessStepResult<CustomerRecord>, Context)>(
            (new BusinessStepResult<CustomerRecord>.Success(input), context));
    }
}
```

### TransformStep

Extends `TechnicalStep<TInput, TOutput, TCtx>`. Converts from the canonical model to the target system's model:

```csharp
public class CustomerTransformer : TransformStep<CustomerRecord, CrmContact, Context>
{
    public override Task<(TechnicalStepResult<CrmContact> Result, Context Context)> ExecuteAsync(
        CustomerRecord input, Context context)
    {
        var contact = new CrmContact(
            ExternalId: int.Parse(input.Id),
            FullName: input.Name);

        return Task.FromResult<(TechnicalStepResult<CrmContact>, Context)>(
            (new TechnicalStepResult<CrmContact>.Success(contact), context));
    }
}
```

### SendStep

Extends `TechnicalStep<T, T, TCtx>`. Sends the transformed data to the external system. The send step can enrich `TOutput` with the external system's response before returning it — this enriched version is what subsequent steps (idempotency record, receipt sender) receive:

```csharp
public class CrmApiSender : SendStep<CrmContact, Context>
{
    private readonly HttpClient _httpClient;

    public CrmApiSender(HttpClient httpClient) => _httpClient = httpClient;

    public override async Task<(TechnicalStepResult<CrmContact> Result, Context Context)> ExecuteAsync(
        CrmContact input, Context context)
    {
        var json = JsonSerializer.Serialize(input);
        var response = await _httpClient.PostAsync("/api/contacts",
            new StringContent(json, Encoding.UTF8, "application/json"));

        response.EnsureSuccessStatusCode();

        // Enrich TOutput with the external system's response
        var responseBody = await response.Content.ReadFromJsonAsync<CrmCreateResponse>();
        var enriched = input with { ExternalId = responseBody!.Id };

        return (new TechnicalStepResult<CrmContact>.Success(enriched), context);
    }
}
```

## Idempotency

The Loader's idempotency strategy is based on CloudEvent metadata, which means you don't need to provide ID or date extractors like you do with the Extractor or Send Pipeline.

The idempotency checker extracts:

- **ID** from `CloudEvent.Subject` (stored in context by `DeserializeStep`)
- **Date** from `CloudEvent.Time` (stored in context by `DeserializeStep`)
- **Hash** from the deserialized `TInput` payload

If either `Subject` or `Time` is missing from the CloudEvent, the pipeline fails with a `TechnicalFailure`.

### Hash generation

The hash is generated in one of three ways (in priority order):

1. **Custom hash generator** — a `Func<TInput, string>` passed to `WithIdempotency()`, used directly as the hash value
2. **`IHashable` interface** — if `TInput` implements `IHashable`, `GetHashString()` is called and SHA256-hashed
3. **Default JSON serialization** — the input is JSON-serialized and the result is SHA256-hashed

For production use, prefer option 1 or 2 for predictable, stable hashes. The default JSON serialization (option 3) is sensitive to serializer settings and property ordering, which can produce different hashes for semantically identical objects:

```csharp
// Option 1: Custom hash generator
.WithIdempotency(hashGenerator: record => $"{record.Id}|{record.Name}")

// Option 2: IHashable interface
public class CustomerRecord : IHashable
{
    public required string Id { get; init; }
    public required string Name { get; init; }
    public string GetHashString() => $"{Id}|{Name}";
}
```

## Receipt sending (optional)

After the data is loaded into the external system, the Loader can optionally send a receipt. This is useful for notifying downstream systems that a record was successfully loaded.

The receipt sender is a regular `SendStep<TOutput, TCtx>` that runs after idempotency is recorded. Because it's a pipeline step (not a finalizer), it only executes on success — if any earlier step fails, the receipt is not sent. If the receipt sender itself fails, the pipeline result becomes a `TechnicalFailure`.

```csharp
.WithReceiptSender(new LoadReceiptPublisher(daprClient))
```

The receipt sender receives the `TOutput` that came out of the send step. If the send step enriched the output with the external system's response (e.g., an assigned external ID), the receipt sender sees that enriched version. This means the receipt can include information from the external system's response without any extra wiring.

```csharp
public class LoadReceiptPublisher : SendStep<CrmContact, Context>
{
    private readonly DaprClient _daprClient;

    public LoadReceiptPublisher(DaprClient daprClient) => _daprClient = daprClient;

    public override async Task<(TechnicalStepResult<CrmContact> Result, Context Context)> ExecuteAsync(
        CrmContact input, Context context)
    {
        // input.ExternalId was set by the send step from the CRM API response
        var cloudEvent = new CloudEvent
        {
            Subject = input.ExternalId.ToString(),
            Time = DateTimeOffset.UtcNow,
            Source = new Uri("urn:company:system:loader"),
            Type = "com.company.contact.loaded",
            Data = JsonSerializer.Serialize(input),
            DataContentType = "application/json"
        };

        await _daprClient.PublishEventAsync("pubsub", "load-receipts", cloudEvent);

        return (new TechnicalStepResult<CrmContact>.Success(input), context);
    }
}
```

## Building a Loader

The `LoaderBuilder` uses `IServiceProvider` to resolve infrastructure services internally. You pass the service provider at creation time:

```csharp
var loader = LoaderBuilder<CustomerRecord, CrmContact, Context>
    .Create("customer-loader", serviceProvider)
    .WithDeserializer(new CustomerDeserializer())
    .WithExtractor(new EnrichWithAddress(addressService))
    .WithValidator(new CustomerValidator())
    .WithIdempotency()
    .WithTransformer(new CustomerTransformer())
    .WithSender(new CrmApiSender(httpClient))
    .WithBusinessIncidents(ctx => ctx.Metadata["cloudevent.id"])
    // Optional receipt sending:
    .WithReceiptSender(new LoadReceiptPublisher(daprClient))
    .Build();
```

All builder methods except `WithExtractor` and `WithReceiptSender` are required. The builder throws `InvalidOperationException` on `Build()` if any required step is missing.

### DI requirements

The builder resolves these services from the `IServiceProvider`:

| Service | Required by | Registration |
|---------|------------|--------------|
| `ILoggerFactory` | Always | `builder.Services.AddLogging()` |
| `FrameworkOptions` | Always | `services.AddIntropyFramework(...)` |
| `IIdempotencyServiceClient` | `WithIdempotency` | Your idempotency service registration |
| `IBusinessIncidentServiceClient` | `WithBusinessIncidents` | Your business incident service registration |

## Executing the Loader

Call `Execute` with a CloudEvent and a context:

```csharp
var context = new Context(new Dictionary<string, string>());

var (result, outputContext) = await loader.Execute(cloudEvent, context);

switch (result)
{
    case StepResult<CrmContact>.Success success:
        // success.Value is the sent object
        break;
    case StepResult<CrmContact>.Cancelled:
        // Idempotency check found this CloudEvent was already processed
        break;
    case StepResult<CrmContact>.BusinessFailure bf:
        // bf.Value is a BusinessIncident (routed automatically by the finalizer)
        break;
    case StepResult<CrmContact>.TechnicalFailure tf:
        // tf.Value is a TechnicalFailure
        break;
}
```

## Related

- [Extractor](extractor.md) — the counterpart that publishes CloudEvents from a source system
- [Transactional Integration](transactional-integration.md) — the two-pipeline alternative with receive and send
- [Result Types](../concepts/result-types.md) — understanding the four result variants
