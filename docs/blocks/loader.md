# Loader

> A standalone pipeline that receives a message's payload, transforms it, and loads it into an external system.

## How it works

```mermaid
graph LR
    A[Payload bytes] --> B[Deserialize]
    B --> C["Extract(s)"]
    C --> D[Validate]
    D --> E[Idempotency Check]
    E --> F[Transform]
    F --> G[Send]
    G --> H[Idempotency Record]
    H --> I["Send Receipt (optional)"]
    I --> J[Business Incident Route]
```


The Loader is a standalone pipeline for loading canonical data into a target system. It takes the message's **payload bytes** as input, deserializes them, optionally enriches the result via extract steps, validates it, transforms it to the target model, and sends it to the external system. The output is a `StepResult<TOutput>`.

The message's own attributes — id, subject, time, source, type, data content type — travel on the context, not through the pipeline. Whoever runs the pipeline puts them there first: the hosting layer projects them from each delivery, and the `CloudEvent` overload of `Execute` does it for a standalone caller.

You can call the Loader's `Execute` method directly — there is no lifecycle manager or message subscription involved. The Loader handles its own OpenTelemetry tracing, idempotency, and business incident routing. By default an execution continues the ambient trace, which is what a host sets up per message; pass `detachTrace: true` to start its own instead.

The Loader is the natural counterpart to the Extractor: where the Extractor reads from a source and publishes CloudEvents, the Loader subscribes to CloudEvents and writes to a target. Together they form the two halves of a Golden Record pipeline.

!!! note "Step namespace"
    The Loader has its own step abstractions in
    `Intropy.Framework.Blocks.Loader.Steps`. Make sure you import from
    this namespace — other blocks have separate step types with the same names.

## Pipeline flow

The pipeline executes in this order:

1. **Deserialize** (`ReadOnlyMemory<byte> → TInput`) — Deserialize the payload bytes into a typed object
2. **Extract** (`TInput → TInput`) — Enrich with additional data (optional, multiple allowed)
3. **Validate** (`TInput → TInput`) — Validate the enriched object
4. **Idempotency Check** (`TInput → TInput`) — Check if this message was already processed (uses `Subject` and `Time` from the context)
5. **Transform** (`TInput → TOutput`) — Convert to the target system's model
6. **Send** (`TOutput → TOutput`) — Send the data to the external system
7. **Idempotency Record** — Record that this CloudEvent was processed (finalizer)
8. **Send Receipt** (`TOutput → TOutput`) — Send a receipt after a successful load (optional step)
9. **Business Incident Route** — Route any business incidents (finalizer)

Each pipeline execution is traced via `PipelineTracing.ExecuteWithTracing`, continuing the caller's trace unless `detachTrace: true` asks for a new root span linked to it.

## CloudEvent metadata on the context

The message's attributes are stamped onto the context **before any step runs** — by the hosting layer for a consumed message, or by the `CloudEvent` overload of `Execute` for a standalone caller. They are available to every step, deserialization included, via `context.Metadata`. Use the `CloudEventContextKeys` constants rather than the literal strings:

| Context key | CloudEvent property | Example value |
|-------------|-------------------|---------------|
| `cloudevent.id` | `Id` | `"abc-123"` |
| `cloudevent.subject` | `Subject` | `"CUST-001"` |
| `cloudevent.time` | `Time` | `"2025-01-15T10:30:00.0000000Z"` |
| `cloudevent.source` | `Source` | `"urn:company:system:crm"` |
| `cloudevent.type` | `Type` | `"com.company.customer.extracted"` |
| `cloudevent.datacontenttype` | `DataContentType` | `"application/json"` |

The idempotency checker uses `cloudevent.subject` as the ID and `cloudevent.time` as the date, so both must be on the context — which means both must be set on the incoming message. An attribute the message does not carry is left off the context rather than written empty, so a missing one fails where it is required instead of silently keying a record on nothing.

## Implementing steps

### DeserializeStep

Extends `BusinessStep<ReadOnlyMemory<byte>, TInput, TCtx>`. It receives the bytes of the message's `data` member exactly as the publisher encoded them:

```csharp
using Intropy.Framework.Blocks.Loader.Steps;

public class CustomerDeserializer : DeserializeStep<CustomerRecord, Context>
{
    // The framework publishes camelCase payloads.
    private static readonly JsonSerializerOptions s_options = new(JsonSerializerDefaults.Web);

    public override Task<(BusinessStepResult<CustomerRecord> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct)
    {
        var record = JsonSerializer.Deserialize<CustomerRecord>(input.Span, s_options)!;
        return Task.FromResult<(BusinessStepResult<CustomerRecord>, Context)>(
            (new BusinessStepResult<CustomerRecord>.Success(record), context));
    }
}
```

Need the message's id, subject or content type? Read them from `context.Metadata` with the
[`CloudEventContextKeys`](#cloudevent-metadata-on-the-context) constants — they are there before this
step runs.

!!! danger "Migrating from the CloudEvent-based DeserializeStep"
    The pipeline's input changed from `CloudEvent` to the payload bytes. Three things to check:

    1. **Override `ExecuteAsync`, not `DeserializeAsync`.** The sealed template is gone and you
       get the `CancellationToken` back. The compiler catches this.
    2. **Read attributes from the context.** Anything you took off the `CloudEvent` — `Id`,
       `Subject`, `Type`, `Source`, `DataContentType` — is now `context.Metadata[CloudEventContextKeys.*]`.
       The compiler catches this too.
    3. **Check what your publisher sets as `Data`.** This one has *no* compile-time signal. The
       payload now arrives verbatim, with nothing unwrapped. If the publisher assigns
       pre-serialized text (`Data = JsonSerializer.Serialize(order)`) it is publishing a JSON
       **string**, and your deserializer will throw a `JsonException` on the opening `"`. Fix the
       publisher to assign the object (`Data = order`), as the
       [Extractor](extractor.md#serializestep) docs now show.

    Verbatim bytes are what let a payload carried inline and one fetched from a claim store be the
    same bytes.

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

The Loader's idempotency strategy is based on the message's metadata on the context, which means you don't need to provide ID or date extractors like you do with the Extractor or Send Pipeline.

The idempotency checker extracts:

- **ID** from `cloudevent.subject` (stamped on the context before any step runs)
- **Date** from `cloudevent.time` (stamped on the context before any step runs)
- **Hash** from the deserialized `TInput` payload

If either is missing from the context, the pipeline fails with a `TechnicalFailure`.

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
            Data = input,                 // the object itself, not pre-serialized text
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
    .WithBusinessIncidents(ctx => ctx.Metadata[CloudEventContextKeys.Id])
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

`Execute` has two overloads. The primary one takes the payload bytes and a context that **already
carries** the message's attributes — this is what the hosting layer calls, because a consumed
message's payload does not always travel inside the message that names it:

```csharp
var context = new Context(new Dictionary<string, string>
{
    [CloudEventContextKeys.Subject] = "CUST-001",
    [CloudEventContextKeys.Time] = DateTimeOffset.UtcNow.ToString("O")
});

var (result, outputContext) = await loader.Execute(payloadBytes, context);
```

The second takes a whole `CloudEvent`, projects its attributes onto the context and runs its
payload through the pipeline — a convenience for standalone callers and tests:

```csharp
var context = new Context(new Dictionary<string, string>());

var (result, outputContext) = await loader.Execute(cloudEvent, context);

switch (result)
{
    case StepResult<CrmContact>.Success success:
        // success.Value is the sent object
        break;
    case StepResult<CrmContact>.Cancelled:
        // Idempotency check found this message was already processed
        break;
    case StepResult<CrmContact>.BusinessFailure bf:
        // bf.Value is a BusinessIncident (routed automatically by the finalizer)
        break;
    case StepResult<CrmContact>.TechnicalFailure tf:
        // tf.Value is a TechnicalFailure
        break;
}
```

## Claim check

A payload too large for the broker does not ride inside its message. The publisher writes it to a
*claim store* and publishes a pointer instead (the CloudEvents standard `dataref` attribute); the
loader fetches the payload back before the pipeline runs. Register the store and there is nothing
else to configure:

```csharp
builder.Services.AddClaimStore(builder.Configuration);
```

The pipeline never sees the mechanism — its input is the payload either way, so no `DeserializeStep`
and no component code changes. Resolution happens after the message has been routed, so a message no
route handles never pays for a fetch, and it happens before the idempotency check, so a duplicate
still fetches its payload before being cancelled.

Unlike the publishing side, a loader has no claim-check *mode*: it resolves a pointer whenever a
message carries one. That is deliberate, and it is what lets a publisher roll back to
`ClaimCheckMode.Never` while the pointers already on the queue keep draining.

!!! warning "Upgrade and configure the consumers first"

    A loader consuming a claim-checked topic needs the claim store registered, reachable, and
    pointed at the same storage the publisher writes to — *before* that publisher turns its claim
    check on. A pointer arriving at a loader with no store fails with a technical error naming the
    missing store, and the message is redelivered until it dead-letters. Deploy consumers first,
    then enable writing; roll back in the reverse order.

A fetch that fails — a missing payload, an unreachable store — is a technical failure, never a
business one: the message is left for redelivery and the broker dead-letters it once redelivery
gives up. A `dataref` that is not a well-formed claim key is refused without the store being read at
all, because the file adapters join a key onto the store's base path without normalising it.

## Related

- [Extractor](extractor.md) — the counterpart that publishes CloudEvents from a source system
- [Transactional Integration](transactional-integration.md) — the two-pipeline alternative with receive and send
- [Result Types](../concepts/result-types.md) — understanding the four result variants
