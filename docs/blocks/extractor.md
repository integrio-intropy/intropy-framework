# Extractor

> A standalone pipeline that reads source data, transforms it, and publishes it as a CloudEvent.

## How it works

```mermaid
graph LR
    A[byte input] --> B[Deserialize]
    B --> C[Validate]
    C --> D["Extract(s)"]
    D --> E[Idempotency Check]
    E --> F[Transform]
    F --> G[Serialize]
    G --> H[Send]
    H --> I[Idempotency Record]
    I --> J[Business Incident Route]
```

The Extractor is a standalone pipeline for extracting data from a source system, transforming it into a canonical model, and publishing it as a CloudEvent. It takes the raw bytes of the source item as input (`ReadOnlyMemory<byte>`), processes it through a series of typed steps, and outputs a `StepResult<CloudEvent>`.

You call the Extractor's `Execute` method directly with the item's raw bytes — there is no lifecycle manager or message subscription involved. The Extractor handles its own OpenTelemetry tracing (each execution creates a detached trace), idempotency, and business incident routing.

The output is always a `CloudEvent`, which makes the Extractor well suited for publishing canonical data to topics or downstream services. Two built-in senders are included: `DaprTopicPublisher` for pub/sub and `DaprServiceInvoker` for service-to-service calls.

!!! note "Step namespace"
    The Extractor has its own step abstractions in
    `Intropy.Framework.Blocks.Extractor.Steps`. Make sure you import from
    this namespace — other blocks have separate step types with the same names.

## Pipeline flow

The pipeline executes in this order:

1. **Deserialize** (`ReadOnlyMemory<byte> → TInput`) — Parse the raw bytes into a typed object. Nothing decodes them first, so this step owns the encoding
2. **Validate** (`TInput → TInput`) — Validate the parsed object
3. **Extract** (`TInput → TInput`) — Enrich with additional data (optional, multiple allowed)
4. **Idempotency Check** (`TInput → TInput`) — Check if this data was already processed
5. **Transform** (`TInput → TOutput`) — Convert to the output model
6. **Serialize** (`TOutput → CloudEvent`) — Create a CloudEvent from the output
7. **Send** (`CloudEvent → CloudEvent`) — Publish the CloudEvent to a destination
8. **Idempotency Record** — Record that this data was processed (finalizer)
9. **Business Incident Route** — Route any business incidents (finalizer)

Each pipeline execution creates a detached OpenTelemetry trace (a new root span linked to the parent) via `PipelineTracing.ExecuteWithTracing`. `WithDaprTopicPublisher` publishes under a `send {topic}` producer span and puts that span's W3C trace context on the CloudEvent (`traceparent`, `tracestate`), so the consumer can continue the extractor's trace.

## Implementing steps

### DeserializeStep

Extends `BusinessStep<ReadOnlyMemory<byte>, TInput, TCtx>`. Converts the raw bytes to your typed model. The framework does not decode the bytes, so a file in any encoding reaches the step exactly as stored: decode text with the encoding your source uses, or give the bytes to a parser that reads them itself (`JsonSerializer` reads UTF-8, `XmlReader` and `StreamReader` detect a BOM or an XML declaration):

```csharp
using Intropy.Framework.Blocks.Extractor.Steps;

public class CustomerDeserializer : DeserializeStep<CustomerRecord, Context>
{
    public override Task<(BusinessStepResult<CustomerRecord> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context)
    {
        // UTF-8 JSON; for a text source in another encoding, decode first:
        // Encoding.GetEncoding("iso-8859-1").GetString(input.Span)
        var record = JsonSerializer.Deserialize<CustomerRecord>(input.Span)!;
        return Task.FromResult<(BusinessStepResult<CustomerRecord>, Context)>(
            (new BusinessStepResult<CustomerRecord>.Success(record), context));
    }
}
```

### ValidateStep

Extends `BusinessStep<T, T, TCtx>`. Same type in and out:

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

### ExtractStep (optional)

Extends `BusinessStep<TInput, TInput, TCtx>`. Enriches the data with information from external sources. You can add multiple extract steps:

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

### TransformStep

Extends `TechnicalStep<TInput, TOutput, TCtx>`. Converts from the source model to the canonical model:

```csharp
public class CustomerTransformer : TransformStep<CustomerRecord, CanonicalCustomer, Context>
{
    public override Task<(TechnicalStepResult<CanonicalCustomer> Result, Context Context)> ExecuteAsync(
        CustomerRecord input, Context context)
    {
        var canonical = new CanonicalCustomer(
            Id: input.Id,
            FullName: input.Name,
            NormalizedAddress: input.Address?.ToUpper());

        return Task.FromResult<(TechnicalStepResult<CanonicalCustomer>, Context)>(
            (new TechnicalStepResult<CanonicalCustomer>.Success(canonical), context));
    }
}
```

### SerializeStep

Extends `TechnicalStep<T, CloudEvent, TCtx>`. Creates a CloudEvent from the output model. You **must** set `Subject` and `Time` on the CloudEvent — the built-in senders validate these:

```csharp
using CloudNative.CloudEvents;

public class CustomerCloudEventSerializer : SerializeStep<CanonicalCustomer, Context>
{
    public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(
        CanonicalCustomer input, Context context)
    {
        var cloudEvent = new CloudEvent
        {
            Subject = input.Id,                    // required by senders
            Time = DateTimeOffset.UtcNow,          // required by senders
            Data = input,                          // the object itself, not pre-serialized text
            DataContentType = "application/json"
        };

        return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
            (new TechnicalStepResult<CloudEvent>.Success(cloudEvent), context));
    }
}
```

!!! danger "Set `Data` to the object, never to pre-serialized text"
    `Data` is the payload, not its encoding — the formatter serializes it when the event
    is published. Assigning `JsonSerializer.Serialize(input)` publishes a JSON **string**
    (`"data": "{\"orderId\":\"A1\"}"`), quoted and escaped, instead of a JSON object
    (`"data": {"orderId":"A1"}`).

    Consumers receive the `data` member verbatim, so a subscriber deserializing your
    contract fails on the opening `"` with a `JsonException`. Set `Data = input`, or use
    the built-in `CloudEventSerializeStep`, which does.

!!! warning "Required CloudEvent properties"
    The built-in `DaprTopicPublisher` and `DaprServiceInvoker` throw
    `InvalidOperationException` if `Subject` or `Time` is not set on the CloudEvent.
    The `Source` and `Type` are set automatically by the sender from the builder
    configuration.

## Building an Extractor

The `ExtractorBuilder` uses `IServiceProvider` to resolve infrastructure services internally. You pass the service provider at creation time:

```csharp
var extractor = ExtractorBuilder<CustomerRecord, CanonicalCustomer, Context>
    .Create("customer-extractor", serviceProvider)
    .WithDeserializer(new CustomerDeserializer())
    .WithValidator(new CustomerValidator())
    .WithExtractor(new EnrichWithAddress(addressService))
    .WithTransformer(new CustomerTransformer())
    .WithSerializer(new CustomerCloudEventSerializer())
    .WithDaprTopicPublisher(
        pubSubName: "pubsub",
        topicName: "customers",
        source: new Uri("urn:company:system:crm"),
        type: "com.company.customer.extracted")
    .WithIdempotency(
        idExtractor: record => record.Id,
        dateExtractor: record => record.ModifiedDate)
    .WithBusinessIncidents(ctx => ctx.Metadata["message_id"])
    .Build();
```

All builder methods except `WithExtractor` and `WithSender` are required. The builder throws `InvalidOperationException` on `Build()` if any required step is missing.

### DI requirements

The builder resolves these services from the `IServiceProvider`:

| Service | Required by | Registration |
|---------|------------|--------------|
| `ILoggerFactory` | Always | `builder.Services.AddLogging()` |
| `FrameworkOptions` | Always | `services.AddIntropyFramework(...)` |
| `DaprClient` | `WithDaprTopicPublisher` / `WithDaprServiceInvoker` | `services.AddDaprClient()` |
| `IIdempotencyServiceClient` | `WithIdempotency` | Your idempotency service registration |
| `IBusinessIncidentServiceClient` | `WithBusinessIncidents` | Your business incident service registration |

## Sending CloudEvents

The Extractor offers three ways to send CloudEvents:

### Dapr topic publisher (recommended)

Publishes CloudEvents to a Dapr pub/sub topic using `DaprClient.PublishByteEventAsync`:

```csharp
.WithDaprTopicPublisher(
    pubSubName: "pubsub",
    topicName: "customers",
    source: new Uri("urn:company:system:crm"),
    type: "com.company.customer.extracted")
```

The publisher serializes the CloudEvent as `application/cloudevents+json` and publishes the bytes to the topic. It sets `Source` and `Type` from the configured values.

### Dapr service invoker

Sends CloudEvents to another Dapr service's `"ingest"` endpoint via `DaprClient.InvokeMethodAsync`:

```csharp
.WithDaprServiceInvoker(
    appId: "customer-loader",
    source: new Uri("urn:company:system:crm"),
    type: "com.company.customer.extracted")
```

The invoker creates an HTTP POST request with the CloudEvent as the body and invokes `{appId}/ingest`. Only a success status (2xx) counts as delivered. Any other status is a technical failure, so the item is not completed and is retried on the next run.

### Custom sender

For destinations that aren't Dapr pub/sub or service invocation, implement `SendStep<TCtx>` directly:

```csharp
public class HttpSender : SendStep<Context>
{
    private readonly HttpClient _httpClient;

    public HttpSender(HttpClient httpClient) => _httpClient = httpClient;

    public override async Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(
        CloudEvent input, Context context)
    {
        var json = JsonSerializer.Serialize(input);
        await _httpClient.PostAsync("/events",
            new StringContent(json, Encoding.UTF8, "application/cloudevents+json"));

        return (new TechnicalStepResult<CloudEvent>.Success(input), context);
    }
}
```

Then use `.WithSender(new HttpSender(httpClient))` instead of the Dapr methods.

## Claim check

Brokers cap message size — Azure Service Bus Standard at 256 KB, for example — and a payload larger
than that cap cannot ride inside its event. With a *claim store* registered, the extractor writes an
oversized payload to the store and publishes a pointer to it instead (the CloudEvents standard
`dataref` attribute); the consumer fetches the payload back before its pipeline runs. No pipeline on
either side sees the mechanism.

The store is registered on its own, with its adapter configured under `ClaimStore` — the same
settings a port takes (`Kind`, `DaprBindingName`, `BasePath`), validated the same way:

```csharp
builder.Services.AddClaimStore(builder.Configuration);
builder.Services.AddExtractor<In, Out, Context>(
    Pipeline,
    contextFactory,
    configureJob: null,
    configureOptions: opts =>
    {
        opts.ClaimCheckMode = ClaimCheckMode.Auto;   // default
        opts.ClaimCheckThresholdBytes = 230_000;     // required in Auto
    });
```

| Option | Description |
|--------|-------------|
| `ClaimCheckMode` | `Auto` claim-checks above the threshold, `Always` claim-checks everything, `Never` claim-checks nothing |
| `ClaimCheckThresholdBytes` | The largest encoded event published inline. Required in `Auto`, ignored otherwise |

Both are deployment decisions, so bind them from configuration rather than hard-coding them —
`Never` is the rollback, and reaching for it must not need a rebuild:

```csharp
configureOptions: opts => opts.Bind(builder.Configuration.GetSection("Extractor"))
```

`Bind` reads `ClaimCheckMode` and `ClaimCheckThresholdBytes`, leaving alone any key the section does
not set, so calling it first lets code override what a deployment supplied.

Without a claim store registered every payload rides inline whatever the mode. Set the threshold
1–2 KB below the broker's limit rather than at it: the measurement is taken before the trace context
is attached, and Dapr adds attributes of its own (`topic`, `pubsubname`, `traceid`) when it forwards
the event.

!!! warning "A claim-checking extractor constrains its CloudEvent ids"

    The event's `id` names its payload in the claim store, so with the claim check configured every
    id must be one or more letters, digits, `.`, `-` or `_` — and neither `.` nor `..`. The
    built-in `CloudEventSerializeStep` mints GUIDs, which already qualify; a custom serialize step
    setting something like `urn:uuid:…` or `order:42` does not.

    The id is checked on every event that carries data, before its size is considered — so an
    extractor with an unusable id scheme fails on its first event, as a technical failure that
    retries and then dead-letters. That is deliberate: the alternative is discovering the problem
    weeks later, the first time a payload happens to grow past the threshold.

The claim check needs the sender to declare the envelope attributes it supplies — its `Source`, and
the `Type` it falls back to — because it encodes the envelope to measure it, and encoding refuses one
missing either. Both built-in senders declare theirs, however they are configured, so
`WithDaprTopicPublisher(...)` and a publisher resolved from DI through `WithSender(...)` both work.
A [custom sender](#custom-sender) declares none by default and is rejected when the extractor is
built; override `EnvelopeDefaults` on it to make it claim-checkable:

```csharp
public class HttpSender(Uri source, string type) : SendStep<Context>
{
    protected override (Uri Source, string Type)? EnvelopeDefaults => (source, type);
    // ...
}
```

In practice only pub/sub needs this. Service invocation has no broker in the path, so no
message-size limit to work around — whatever limit the target service imposes is its own.

A payload is written before its pointer is published, so a crash can orphan a payload but never
leave a pointer to one that was never written. The framework never deletes a claimed payload: keys
are partitioned by date (`{component}/{yyyy}/{MM}/{dd}/{id}`) so a storage lifecycle policy can
expire them, and configuring that policy is the platform's job.

!!! danger "A claim-checked event has no `data`, so `data.*` subscription filters stop matching it"

    This is a limitation of the pattern, not something the framework works around. A Dapr
    subscription rule that filters on `data.foo` cannot match an event whose payload is in the claim
    store — the message lands on `/unhandled` instead.

    Lift anything used for routing out of the payload and onto a CloudEvent extension attribute,
    which survives claim-checking. The framework already demonstrates this: `intropysourceport` is
    an extension attribute precisely so routing on it keeps working, and routing on the event
    **type** is unaffected either way.

    Before enabling the claim check on a topic, audit its subscriptions for rules that read `data.*`.

### Rolling out and back

Unlike a Transactional Integration, an extractor publishes onto the system bus: its consumers are
*other components*, and the wire between them stops being private.

Roll out in this order: deploy the store and the consumers that can resolve pointers first, then
enable writing. A publisher that claim-checks before its consumers can resolve produces events that
fail until the consumers catch up — each one with a technical error naming the missing store, and
redelivery until they dead-letter.

Roll back in the reverse order, which is why `Never` is a mode rather than an absence of a store:

1. Set `ClaimCheckMode` to `Never`. New payloads stay inline immediately.
2. Leave the store registered on the consumers until the topic has drained every pointer already on
   it. A consumer resolves a pointer whenever an event carries one, whatever this extractor's mode.
3. Remove the store.

## Executing the Extractor

Call `Execute` with the raw bytes and a context:

```csharp
var context = new Context(new Dictionary<string, string>
{
    ["message_id"] = "msg-123"
});

var (result, outputContext) = await extractor.Execute(inputBytes, context);

switch (result)
{
    case StepResult<CloudEvent>.Success success:
        // success.Value is the published CloudEvent
        break;
    case StepResult<CloudEvent>.Cancelled:
        // Idempotency check found this was already processed
        break;
    case StepResult<CloudEvent>.BusinessFailure bf:
        // bf.Value is a BusinessIncident (routed automatically by the finalizer)
        break;
    case StepResult<CloudEvent>.TechnicalFailure tf:
        // tf.Value is a TechnicalFailure
        break;
}
```

## Several source ports and routes

A hosted extractor (see the Hosting README) can sweep several source ports and send each port's
files through the pipeline for its data: for example orders and invoices from the same partner,
delivered to different folders. Each port carries one kind of data, in one format. Declare the ports
as **sources** and the pipelines as **routes**, the same shape as a
[Transactional Integration's send routes](transactional-integration.md#several-source-ports-and-send-routes):

```csharp
builder.Services.AddExtractor<OrderContext>(
    sources => sources
        .Port("orders-in")
        .Port("invoices-in", FileCompletion.Archive("processed")),
    routes => routes
        .On<OrderFile, Order>(["orders-in"], (builder, sp) => builder
            .WithDeserializer<OrderDeserializer>()
            .WithIdempotency((order, _) => order.Id, (order, _) => order.ChangedAt)
            /* validator, transformer, serializer, sender as for any extractor */)
        .On<InvoiceFile, Invoice>(["invoices-in"], (builder, sp) => builder
            .WithDeserializer<InvoiceDeserializer>()
            /* ... */)),
    (metadata, isRetry) => new OrderContext(metadata, isRetry));
```

Each route's pipeline is typed on its own data (`On<TInput, TOutput>`) and configured exactly like
`AddExtractor`'s; only the context type is shared by the whole extractor. On the base `Context`,
leave out the context factory: `AddExtractor(sources, routes)`. Each port's file adapter is
registered keyed by the port name, as for a single port. Don't call `AddSourcePort`: the sources
declare the ports.

A route names the ports whose files it handles: `["orders-in"]`, or several ports that carry the
same data, `["orders-se", "orders-no"]`. When every port carries the same data, declare one route
for all of them with `OnAny`. The startup checks are the Transactional Integration's: no
source or no route, a route naming an undeclared port, a port handled by no route or by more than
one, and `OnAny` combined with routes by port all fail the registration. Before any file is listed,
each port's pipeline is built once, so a route that cannot be composed fails the run naming the
route. The ports are swept one after another, in declaration order; a port that fails does not stop
the others, but the run still fails, naming every port that failed.

### What the routes add to each pipeline

Each route's pipeline is built for one port at a time, and that port changes two things:

- **The published event carries the port.** Every CloudEvent the pipeline sends has the
  `intropysourceport` extension, set by the framework, never from the context. A consumer can
  filter on it; most route on the CloudEvent type instead, which each route sets on its own.
- **Idempotency records are kept per port.** The id is `{port}:{id}`, so order `123` from one port
  and invoice `123` from another are never duplicates of each other, and adding a port never
  changes the records of the others. When several ports deliver copies of the same feed and a
  record seen on one should be a duplicate on all of them, share the records:
  `.WithIdempotency(idExtractor, dateExtractor, scope: IdempotencyScope.Entity)`.

!!! warning "Moving an existing extractor to routes"
    An extractor registered without routes keys its records on the id alone. Moved to routes, its
    ids gain the port prefix, so files that are already recorded would be processed again. Keep the
    old records with `scope: IdempotencyScope.Entity` on that port's route, or accept a one-time
    re-run.

The pipelines are named `{component}.Process.{route}`, where the route is its ports, for example
`orders-extractor.Process.orders-se+orders-no`; an `OnAny` route keeps `{component}.Process`.

## Related

- [Transactional Integration](transactional-integration.md) — the two-pipeline alternative
- [Result Types](../concepts/result-types.md) — understanding the four result variants
