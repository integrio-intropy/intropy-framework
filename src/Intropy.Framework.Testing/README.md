# Intropy.Framework.Testing

Hand-rolled fakes and delivery helpers for component integration tests against
the [Intropy framework](https://github.com/integrio-intropy/intropy-framework).
No mocking-framework dependency, no ASP.NET dependency, no Dapr sidecar
required.

The package fakes the four edges every component integration test fakes, plus a
helper for delivering CloudEvents to loader endpoints exactly as a Dapr sidecar
does:

| Helper | Namespace | Fakes |
|---|---|---|
| `InMemoryFileAdapter` | `Intropy.Framework.Testing.Adapters` | `IFileAdapter` (source/destination connectors) |
| `FakeTopic<TCtx>` | `Intropy.Framework.Testing.Topics` | The extractor's Dapr pub/sub publish step |
| `FakeIdempotencyServiceClient` | `Intropy.Framework.Testing.Services` | `IIdempotencyServiceClient` |
| `FakeBusinessIncidentServiceClient` | `Intropy.Framework.Testing.Services` | `IBusinessIncidentServiceClient` |
| `DaprDelivery` | `Intropy.Framework.Testing.Delivery` | Sidecar CloudEvents delivery to loader endpoints |

## The canonical pattern

1. **Build the component's pipeline/host exactly as production composition
   does** — same steps, same builder calls.
2. **Fake the edges:**
   - Extractors: `ExtractorBuilder<...>.WithSender(fakeTopic)` in place of
     `WithDaprTopicPublisher(...)`.
   - Loaders: override the keyed `IFileAdapter` registration with an
     `InMemoryFileAdapter`, and the two service clients with their fakes
     (plain DI override — last registration wins).
3. **Assert on fake state:** topic count/events, adapter files, recorded
   incidents/resolutions and commits, delivery acks.

```csharp
// Extractor test shape
var topic = new FakeTopic<MyContext>();
var pipeline = MyExtractorComposition.BuildPipeline(
    extractorBuilder => extractorBuilder.WithSender(topic));

await RunSweep(pipeline, fileAdapter); // component's own orchestration

Assert.Equal(2, topic.Count);
Assert.Equal("order.created", topic.Events[0].Type);
```

```csharp
// Loader test shape (WebApplicationFactory with DI overrides)
var client = factory.CreateClient();
var ack = await client.DeliverAsync("/events/orders", cloudEvent);

Assert.Equal(DeliveryAck.Success, ack);
Assert.Equal(expectedJson, destinationFiles.GetString("out/order-42.json"));
```

## Fake semantics (fail like production)

- **`InMemoryFileAdapter`** — missing reads throw `FileNotFoundException` (the
  Dapr binding throws; it never returns null); deletes of missing files no-op
  (binding delete is idempotent); files are keyed on the effective path
  (`basePath/fileName` when a write passes an override). `ReadException` /
  `WriteException` simulate a dead source/destination.
- **`FakeTopic<TCtx>`** — captures the exact `CloudEvent` instances the real
  publisher would have encoded. `SendException` surfaces as a *technical
  failure* through the framework's normal exception handling, matching a dead
  broker.
- **Service fakes** — faults throw the *typed* `IdempotencyServiceException` /
  `BusinessIncidentServiceException`. Typing matters: the framework's incident
  router has a dedicated catch for `BusinessIncidentServiceException`; any
  other exception type takes a different code path than production.
- **`DaprDelivery.DeliverAsync`** — POSTs the structured-mode envelope as
  `application/cloudevents+json` and parses the `{"status": "..."}` ack.
  Unknown, missing, or malformed statuses map to `DeliveryAck.Retry`, matching
  the sidecar's fail-safe redelivery. `DeliveryAck` members map to wire values
  as `Success` ↔ `SUCCESS`, `Retry` ↔ `RETRY`, `Drop` ↔ `DROP`.

## Loader ack/consumption matrix

| Scenario | Ack | Side effects |
|---|---|---|
| Valid message | `SUCCESS` | File written to destination |
| Duplicate (idempotency `Ignore`) | `SUCCESS` | Nothing written |
| Business-rule violation | `SUCCESS` | Incident routed — **consumed, never retried** |
| Destination throws | `RETRY` | Nothing written |
| Idempotency service down (`StatusException`) | `RETRY` | Technical failure |
| Malformed envelope | `RETRY` | No incident routed |

> **Business failures are consumed.** When a business step fails, the
> framework's incident-router finalizer triggers the incident and returns
> `Success(defaultValueFactory())` — for framework-wired extractors an *empty
> `CloudEvent`*, never null. Do not assert on null result values; assert on
> `Incidents` instead.

## Extractor sweep matrix

| Scenario | Published | Source file |
|---|---|---|
| Valid | Yes | Deleted |
| Duplicate (idempotency `Ignore`) | No | Still deleted |
| Validation failure | Incident routed | Consumed (empty-`CloudEvent` success) |
| Technical failure (`SendException` / `ReadException`) | No | Left for the next run |

## Notes

- Thread-safe: all recorded state is lock-guarded and exposed as snapshots, so
  parallel extractor sweeps cannot corrupt assertions.
- Versioned in lockstep with the framework packages.
