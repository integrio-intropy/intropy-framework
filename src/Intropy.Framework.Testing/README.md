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
| `FakeEnqueueStep<TCtx>` | `Intropy.Framework.Testing.Topics` | The transactional receive pipeline's queue publish step |
| `FakeIdempotencyServiceClient` | `Intropy.Framework.Testing.Services` | `IIdempotencyServiceClient` |
| `FakeBusinessIncidentServiceClient` | `Intropy.Framework.Testing.Services` | `IBusinessIncidentServiceClient` |
| `DaprDelivery` | `Intropy.Framework.Testing.Delivery` | Sidecar CloudEvents delivery to loader endpoints |
| `PublishedMessageCapture` | `Intropy.Framework.Testing.Dapr` | Publish-call capture for `DaprClient` substitutes |

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
  (`basePath/fileName` when a write passes an override). Pass a `basePath` to
  the constructor to make `ListAsync` mirror `LocalFileAdapter`: only files
  under that path, listed by file name. `ReadException` /
  `WriteException` / `DeleteException` simulate a dead source/destination.
  Per-file faults: `AddUnreadableFile(name)` seeds a file that is listed but
  throws on read (corrupt source file), and `SetDeleteException(name, ex)`
  makes deletes fail for one file while others succeed — the
  publish-succeeds-but-delete-fails path, where the file is re-processed next
  run and idempotency must catch it.
- **`FakeTopic<TCtx>`** — captures the exact `CloudEvent` instances the real
  publisher would have encoded. `SendException` surfaces as a *technical
  failure* through the framework's normal exception handling, matching a dead
  broker.
- **`FakeEnqueueStep<TCtx>`** — the transactional receive pipeline's enqueue
  seam, plugged in via `ReceivePipelineBuilder<TCtx>.WithEnqueuer(fake)`.
  Captures each `SourceItem` and its already-encoded structured-mode CloudEvents
  envelope (defensive `byte[]` copies — the formatter's buffer is recycled);
  decode via `CapturedEnqueue.DecodeCloudEvent()`. Item identity survives into
  the capture, so completer-failure tests can assert what was enqueued even
  when the pipeline result is a business failure. `SendException` is thrown
  before capture, surfaces as a *technical failure*, and — because the complete
  step follows the enqueue step — leaves the source file undeleted, matching a
  dead broker in production. The `EnqueueStep` API this fake plugs into is
  documented in `docs/blocks/transactional-integration.md`; the
  `DaprEnqueuer` sample in the block's README is stale (fixed separately).
- **Service fakes** — faults throw the *typed* `IdempotencyServiceException` /
  `BusinessIncidentServiceException`. Typing matters: the framework's incident
  router has a dedicated catch for `BusinessIncidentServiceException`; any
  other exception type takes a different code path than production.
- **`DaprDelivery.DeliverAsync`** — POSTs the structured-mode envelope as
  `application/cloudevents+json` and parses the `{"status": "..."}` ack.
  Unknown, missing, or malformed statuses map to `DeliveryAck.Retry`, matching
  the sidecar's fail-safe redelivery. `DeliveryAck` members map to wire values
  as `Success` ↔ `SUCCESS`, `Retry` ↔ `RETRY`, `Drop` ↔ `DROP`.
- **`PublishedMessageCapture`** — records publish calls made through a
  `DaprClient` substitute configured by your test project (works with any
  mocking framework; one wiring line per fake). Assert on the captured
  `PublishedMessage`s and decode envelopes via `DecodeCloudEvent()`:

  ```csharp
  var daprClient = Substitute.For<DaprClient>();
  var capture = new PublishedMessageCapture();
  daprClient
      .PublishByteEventAsync(
          Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
          Arg.Any<string?>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
      .Returns(Task.CompletedTask)
      .AndDoes(ci => capture.Capture(
          ci.Arg<string>(), ci.ArgAt<string>(1), ci.ArgAt<ReadOnlyMemory<byte>>(2),
          ci.ArgAt<string?>(3)));

  // ... run the pipeline ...

  var message = capture.Messages.Single();
  Assert.Equal("orders", message.TopicName);
  Assert.Equal("order-42", message.DecodeCloudEvent().Subject);
  ```

## Loader ack/consumption matrix

> This matrix describes the behavior of framework-composed loader and
> transactional pipelines (which acks their endpoints/handlers produce), not
> of this package. `DaprDelivery` only delivers the envelope and parses the
> ack.

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

## Transactional receive pipeline matrix

Per source item:

| Scenario | Enqueued | Source file |
|---|---|---|
| Valid | Yes | Deleted by completer |
| Unreadable source (adapter read throws)¹ | No | Incident routed, keyed on file name |
| Broker down (`SendException` set) | No — technical failure | Left for the next run |
| Enqueue OK, delete throws (`SetDeleteException`) | Yes — the duplicate lands on the queue | Left; re-processed next run, send-side idempotency must absorb it |

¹ `ReceiveStep` is a `BusinessStep`, so adapter faults here are *business*
failures — unlike the loader's technical read path.

The fourth row is the transactional analogue of
`InMemoryFileAdapter.SetDeleteException` and the most valuable row in the
matrix: the enqueue is captured, the pipeline result is a business failure,
and the file is re-processed next run.

## Transactional send pipeline matrix

Per delivered message:

| Scenario | Written | Idempotency |
|---|---|---|
| Valid | Yes | Committed |
| Duplicate (idempotency `Ignore`) | No | No new commit |
| Validation failure | No — incident routed, consumed | No commit |
| Destination throws | No — business failure, incident routed² | No commit |

² Transactional `SendStep` is a `BusinessStep`; an uncaught exception routes an
incident, it is *not* a technical failure. This is why this package ships no
`FakeSendStep` for the send pipeline: a `FakeTopic`-style `SendException` knob
would surface as a business incident and mis-model production. Assert through
the component's real sender + `InMemoryFileAdapter` instead — it tests more,
including the file-naming convention.

## Notes

- Thread-safe: all recorded state is lock-guarded and exposed as snapshots, so
  parallel extractor sweeps cannot corrupt assertions. Fault knobs
  (`ReadException`, `SendException`, ...) are `volatile` — safe to toggle
  between runs, not a coordination primitive for mid-run assertions.
- Versioned in lockstep with the framework packages.
