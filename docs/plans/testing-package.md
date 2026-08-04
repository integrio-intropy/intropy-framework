# Plan: Intropy.Framework.Testing (v2)

A companion test-helpers package for the Intropy framework. It provides
hand-rolled fakes for the four edges every component integration test fakes:
the file adapter, the publish topic, the idempotency service client, and the
business-incident service client — plus a helper for delivering CloudEvents to
loader endpoints exactly as a Dapr sidecar does.

This plan is self-contained: everything needed to implement it (API shapes,
framework semantics, pitfalls) is specified below.

## Background: the framework pieces being faked

The Intropy framework (packages `Intropy.Framework.Blocks`,
`Intropy.Framework.Adapters`, `Intropy.Framework.Core`, `Intropy.Contracts`)
provides pipeline builders for two component archetypes:

- **Extractors** — run-to-completion jobs: read files from a source connector
  (a Dapr `bindings.localstorage` component behind `IFileAdapter`), run a
  pipeline (deserialize → validate → idempotency-check → transform → serialize
  → send → idempotency-record → incident-route), and publish the result as a
  CloudEvent to a Dapr pub/sub topic.
- **Loaders** — long-running ASP.NET services: the Dapr sidecar POSTs topic
  deliveries to a subscription endpoint as CloudEvents envelopes
  (`application/cloudevents+json`), a pipeline (deserialize → validate →
  idempotency → transform → send) writes to a destination connector (also
  `IFileAdapter`), and the endpoint answers with an ack body
  (`{"status":"SUCCESS"}` consumes the message, `{"status":"RETRY"}` asks the
  sidecar for redelivery). **Note:** the framework ships no subscription
  endpoint today; loader endpoints are component-owned. `DaprDelivery`
  (deliverable 4) encodes the Dapr delivery *convention* these endpoints
  implement.

### Framework API facts the implementation relies on (verified against the repo and `Intropy.Contracts` 0.1.0)

- `IFileAdapter` (namespace `Intropy.Framework.Adapters.File`) — 6 members:
  `Task<List<FileInfo>> ListAsync()`,
  `Task<byte[]> GetContentAsync(string)` — **non-nullable**; only the
  `Encoding` overload is `Task<string?>`,
  `Task<string?> GetContentAsync(string, Encoding)`,
  `Task WriteAsync(string, byte[], string? basePathOverride = null)`,
  `Task WriteAsync(string, string, Encoding, string? basePathOverride = null)`,
  `Task DeleteAsync(string)`.
  `Intropy.Framework.Adapters.Common.FileInfo` is
  `record FileInfo(string FileName)`.
  The production implementation is `LocalFileAdapter`, which shells out to a
  Dapr binding — tests must never touch it.
  **Missing-file behavior (production):** the Dapr binding *throws* on a
  missing file; it never returns null. `LocalFileAdapter.GetContentAsync(
  fileName, encoding)` delegates to the byte[] overload and calls
  `encoding.GetString(data)`. `DeleteAsync` on a missing file succeeds
  silently (binding delete is idempotent).
- `ExtractorBuilder<TInput, TOutput, TCtx>` (namespace
  `Intropy.Framework.Blocks.Extractor`) has `WithSender(SendStep<TCtx>)` as an
  alternative to `WithDaprTopicPublisher(pubSubName, topicName, source, type)`.
  The real publisher is an internal step that encodes the CloudEvent with
  `CloudNative.CloudEvents.SystemTextJson.JsonEventFormatter` (structured
  mode) and calls `DaprClient.PublishByteEventAsync(pubsub, topic, bytes,
  "application/cloudevents+json", ...)`.
- The extractor-side send step abstraction is
  `Intropy.Framework.Blocks.Extractor.Steps.SendStep<TCtx> :
  TechnicalStep<CloudEvent, CloudEvent, TCtx> where TCtx : Context`, with
  `public abstract Task<(TechnicalStepResult<CloudEvent> Result, TCtx Context)>
  ExecuteAsync(CloudEvent input, TCtx context, CancellationToken ct)`.
  `Context` is `Intropy.Framework.Blocks.Shared.Context`. (There are two other
  `SendStep` classes in the assembly — pick the one in the `Extractor.Steps`
  namespace taking `CloudEvent`.)
  **Verified:** `WithSender` takes the builder's own `TCtx`, so a generic
  `FakeTopic<TCtx>` composes for any component context derived from `Context`.
  No spike needed.
- `IIdempotencyServiceClient` (namespace
  `Intropy.Contracts.IdempotencyService`) — 3 members:
  `Task<MessageInfo?> GetInfoAsync(string component, string id,
  CancellationToken)`,
  `Task<StatusResponse> GetStatusAsync(MessageInfo, CancellationToken)`,
  `Task CommitAsync(MessageInfo, CancellationToken)`.
  `MessageInfo` is `record MessageInfo(string Component, string Id, string
  Hash, DateTimeOffset Timestamp)`.
  `StatusResponse` is `record StatusResponse(Action Action, Reason Reason)`
  with `enum Action { Proceed, Ignore }` and `enum Reason { NewerData,
  SameData, StaleData, NoPreviousData }`.
  The framework's idempotency-check step (`ExternalIdempotencyChecker`)
  **never calls `GetInfoAsync`** — it builds a fresh `MessageInfo` and calls
  `GetStatusAsync`, cancelling the message when `Action == Ignore`. Its
  catch-all converts any client exception to a technical failure.
- `IBusinessIncidentServiceClient` (namespace
  `Intropy.Contracts.BusinessIncidentService`) — members:
  `Task Trigger(Uri source, string subject, string id, BusinessIncidentData
  data, string? batchId)`,
  `Task Resolve(Uri source, string subject, string id, string? batchId)`,
  `Task<IncidentResponse> GetById(Guid)`,
  `Task<IncidentListResponse> List(IncidentFilter)`,
  `Task<List<IncidentEventResponse>> GetEvents(Guid)`,
  `Task<bool> ResolveManual(Guid)`.
  `BusinessIncidentData` is a sealed record with settable `Description` and
  `Context` (`Dictionary<string, string>`) properties.
- Pipeline semantics for business failures (verified against the framework
  source): when a business step returns `BusinessFailure(
  BusinessIncidentData)`, the pipeline short-circuits and the
  business-incident-router **finalizer** (`BusinessIncidentRouteStep`,
  triggers `OnSuccess | OnBusinessFailure`) calls `Trigger(...)` on the
  client, then returns **`Success(defaultValueFactory())`** — for
  framework-wired extractors the factory is `() => new CloudEvent()`, i.e. an
  **empty `CloudEvent`, never null**. So a validation failure surfaces to the
  component as *success with an empty value* — the message is consumed, never
  retried. Tests and helpers must treat "incident routed" as a consumed
  message, and must not assert on null result values.
- **Failure typing matters:** `ExternalBusinessIncidentRouter` has a dedicated
  catch for `BusinessIncidentServiceException` (converts to
  `TechnicalFailure`); any other exception type falls through to the generic
  catch in `Finalizer.ExecuteAsyncInternal`. Fakes must throw the *typed*
  service exceptions so tests exercise the same code path as production.
- The router also calls `Resolve(...)` on the success path when
  `context.IsRetry` is true.
- `Context` is `record Context(Dictionary<string, string> Metadata,
  bool IsRetry = false)`. Loader idempotency keys off context metadata stamped
  by the component's deserializer.

## Design principles

1. **Hand-rolled fakes, no mocking-framework dependency.** The package must
   not reference NSubstitute/Moq; consumers keep their own choice.
2. **Framework types only.** Helpers know nothing about any component's
   contracts, models, or constants.
3. **No ASP.NET dependency.** The delivery helper works off `HttpClient` so
   the package stays usable from plain console test hosts.
4. **Lockstep versioning with the framework (0.x).** Helpers subclass
   framework steps — the API surface most likely to shift. Release CI packs
   everything in the solution with the git-tag version, so lockstep is
   automatic once the project is in the slnx.
5. **Fakes fail like production.** Missing files throw; service faults throw
   the typed `*ServiceException`; the wrong thing is unrepresentable where
   possible.

## Package shape

- New project `src/Intropy.Framework.Testing`, `net10.0`, packable, in
  `Intropy.Framework.slnx` alongside the other `Intropy.Framework.*` packages.
- Package references: `Intropy.Framework.Blocks` (project reference —
  transitively brings Adapters, Core, `Intropy.Contracts`, and
  `CloudNative.CloudEvents.SystemTextJson`). Add explicit
  `Intropy.Framework.Adapters` and `Intropy.Contracts` references only if
  direct-reference clarity is preferred; nothing else.
- Namespaces mirror the faked edge: `Intropy.Framework.Testing.Adapters`,
  `.Topics`, `.Services`, `.Delivery`.
- The repo's `Directory.Build.props` enforces `AnalysisMode=All` +
  warnings-as-errors. Expect a handful of targeted `[SuppressMessage]`
  attributes (settable exception properties, static helper class). Do **not**
  add broad `NoWarn` to the new csproj.

## Deliverables

### 1. `InMemoryFileAdapter` (`Intropy.Framework.Testing.Adapters`)

Stateful dictionary-backed `IFileAdapter` fake.

**Storage:** keyed on the *effective path* — `basePathOverride + "/" +
fileName` when an override is passed (mirroring
`LocalFileAdapter.CombinePath`), else just `fileName`. Ordinal-ignore-case
comparer. A `lock` guards all mutations/reads.

**Seeding:** `AddFile(string fileName, string content)` (UTF-8, fluent) and
`AddFile(string fileName, byte[] content)`.

**Assertions:** `Files` (`IReadOnlyDictionary<string, byte[]>` snapshot keyed
by effective path), `GetString(string fileName)` (UTF-8 decode of the
plain-key entry), and `GetString(string basePath, string fileName)` for
override-written files.

**Fault injection:** `Exception? ReadException { get; set; }` (thrown by
list/get — dead source) and `Exception? WriteException { get; set; }` (thrown
by write — dead destination); clearing the property restores normal behavior.

**Contract decisions (match production):**

- `GetContentAsync(string)` for a missing file **throws
  `FileNotFoundException`** (the real adapter throws via the Dapr binding; it
  never returns null). The `Encoding` overload delegates to the byte[]
  overload, exactly like `LocalFileAdapter`.
- `DeleteAsync` for a missing file **no-ops** (binding delete is idempotent in
  production).
- `ListAsync` returns all files in the store. Divergence from
  `LocalFileAdapter` (which filters to its configured `BasePath`): the fake
  has no configured base path — document this.

Reference behavior: the sweep-style scenario List → GetContent → Delete must
behave consistently against the same dictionary; that statefulness is why a
fake beats per-call substitutes.

### 2. `FakeTopic<TCtx>` (`Intropy.Framework.Testing.Topics`)

```csharp
public class FakeTopic<TCtx> : Intropy.Framework.Blocks.Extractor.Steps.SendStep<TCtx>
    where TCtx : Intropy.Framework.Blocks.Shared.Context
{
    public int Count { get; }                          // published count
    public IReadOnlyList<CloudEvent> Events { get; }   // publication order (snapshot)
    public Exception? SendException { get; set; }      // dead-broker simulation
    // ExecuteAsync: throw SendException if set; else capture input and
    // return new TechnicalStepResult<CloudEvent>.Success(input)
}
```

Plugged in via `ExtractorBuilder<...>.WithSender(fakeTopic)` in place of
`WithDaprTopicPublisher`. A test builds the component's pipeline with the real
steps and this fake at the edge, executes it, then asserts on `Count` (e.g.
"2 of 3 files published") and on envelope identity (`Events[i].Subject`,
`.Time`, `.Data`) — the exact `CloudEvent` instances the real publisher would
have encoded.

`FakeTopic` is a `TechnicalStep`: throwing `SendException` surfaces as a
technical failure through the framework's normal exception handling, matching
a dead broker in production.

### 3. Platform-service fakes (`Intropy.Framework.Testing.Services`)

```csharp
public class FakeIdempotencyServiceClient : IIdempotencyServiceClient
{
    public StatusResponse NextStatus { get; set; }
        // Sticky fallback. Default: new StatusResponse(Action.Proceed, Reason.NoPreviousData)
    public void QueueStatus(params StatusResponse[] statuses);
        // When the queue is non-empty, GetStatusAsync dequeues in order;
        // when empty, falls back to NextStatus. Covers "first check proceeds,
        // second check ignores" sequences with one fake.
    public IReadOnlyList<MessageInfo> StatusChecks { get; }   // recorded GetStatusAsync args
    public IReadOnlyList<MessageInfo> Committed { get; }      // recorded CommitAsync args
    public IdempotencyServiceException? StatusException { get; set; }  // service-down simulation
    // Typed exception: ExternalIdempotencyChecker catches all, but typing keeps
    // the fake honest with the real client's failure mode.
    // CommitAsync stores into an internal store so GetInfoAsync(component, id)
    // returns the committed MessageInfo; default (nothing committed) is null.
}

public class FakeBusinessIncidentServiceClient : IBusinessIncidentServiceClient
{
    public IReadOnlyList<RecordedIncident> Incidents { get; } // every Trigger call
    public IReadOnlyList<ResolvedIncident> Resolved { get; }  // every Resolve call
    public BusinessIncidentServiceException? TriggerException { get; set; }  // service-down
    // Typed exception: ExternalBusinessIncidentRouter has a dedicated catch for
    // BusinessIncidentServiceException — any other type would take a different
    // (generic-catch) code path than production.
    // Trigger records the incident. Resolve always appends to Resolved AND marks
    // the matching recorded incident resolved; unmatched resolve is recorded and
    // otherwise no-ops (never throws).
    // GetById/List/GetEvents/ResolveManual serve from the recorded lists.
}

public sealed record RecordedIncident(
    Uri Source, string Subject, string Id, BusinessIncidentData Data, string? BatchId);

public sealed record ResolvedIncident(
    Uri Source, string Subject, string Id, string? BatchId);
```

The recorded lists are the payoff over mocking frameworks: tests assert on
incident *contents* directly, e.g. `fake.Incidents.Single().Data.Description`,
and on the retry-resolve path via `fake.Resolved`.

All recorded lists are `List<T>` + `lock` internally, exposed as
`IReadOnlyList<T>` snapshots (not `ConcurrentQueue` — enumeration order
matters for assertions, and extractor sweeps may process files in parallel).

### 4. CloudEvents delivery helper (`Intropy.Framework.Testing.Delivery`)

```csharp
public enum DeliveryAck { Success, Retry, Drop }
// PascalCase members map to Dapr's wire values SUCCESS / RETRY / DROP.
// Mapping documented in XML docs and README.

public static class DaprDelivery
{
    // Structured-mode envelope JSON via JsonEventFormatter, the same encoding
    // the framework publisher produces.
    public static string ToDeliveryEnvelope(CloudEvent cloudEvent);

    // POSTs the envelope with content type application/cloudevents+json and
    // parses the {"status": "..."} ack body into DeliveryAck.
    // Unknown, missing, or malformed status values map to Retry — matching the
    // sidecar, which redelivers on any status it doesn't recognize (fail-safe).
    public static Task<DeliveryAck> DeliverAsync(
        this HttpClient client, string route, CloudEvent cloudEvent,
        CancellationToken ct = default);
}
```

Why this exists: Dapr HTTP delivery has non-obvious semantics every loader
test otherwise re-derives — the envelope is `application/cloudevents+json`
(which ASP.NET's JSON binder rejects, so subscription handlers parse the body
manually), the sidecar expects a 200 with a `status` field, and payload is in
the envelope's `data` member. The helper encodes that once. The framework
ships no subscription endpoint itself — this helper encodes the Dapr delivery
convention that component-owned endpoints implement.

Loader tests then look like: `WebApplicationFactory<Program>` with the
destination `IFileAdapter` keyed registration and the two service clients
overridden (last registration wins), `CreateClient()`, and
`await client.DeliverAsync("/events/<topic-route>", cloudEvent)` asserting the
ack plus fake state.

### 5. Package self-tests

New project `test/Intropy.Framework.Testing.Test`, mirroring the existing
one-test-project-per-package convention.

- `InMemoryFileAdapter`: seed/list/get/delete round-trip; **missing file
  throws `FileNotFoundException`** on both `GetContentAsync` overloads;
  delete-missing no-ops; `basePathOverride` writes are stored and assertable
  under the effective path; `ReadException`/`WriteException` throw and
  recovery after clearing.
- `FakeTopic<TCtx>`: capture order, count, exception-passthrough-to-technical-
  failure. Use the framework's own `Context` as `TCtx`.
- `FakeIdempotencyServiceClient`: default proceed; sticky `NextStatus`
  honored; `QueueStatus` sequence honored then falls back to `NextStatus`;
  commit recorded and visible via `GetInfoAsync`; `StatusException` propagates
  as the typed exception.
- `FakeBusinessIncidentServiceClient`: trigger recorded with all fields;
  resolve appends to `Resolved` and marks the matching incident; unmatched
  resolve is recorded without throwing; `TriggerException` propagates as
  `BusinessIncidentServiceException`.
- `DaprDelivery`: envelope round-trip (encode → decode with
  `JsonEventFormatter` → same id/subject/time/data); ack parsing for
  SUCCESS/RETRY/DROP/**unknown/missing → Retry**.

### 6. Reference usage guide (README in the package)

Document the canonical integration-test pattern (documentation, not code — see
non-deliverables):

1. Build the component's pipeline/host exactly as production composition does
   (same steps, same builder calls).
2. Fake the edges: `WithSender(fakeTopic)` for extractors; keyed-service DI
   override for `IFileAdapter`; plain DI override for the two service clients
   (last registration wins).
3. Assert on fake state: topic count/events, adapter files, recorded
   incidents/resolutions and commits, delivery acks.
4. The ack/consumption matrix every loader should pin down: valid → SUCCESS +
   file written; duplicate → SUCCESS + nothing written; business-rule
   violation → SUCCESS + incident routed (consumed, never retried — the router
   returns `Success(defaultValueFactory())`, an **empty** value, not null);
   destination throws → RETRY + nothing written; idempotency-service down
   (`StatusException`) → technical failure → RETRY; malformed envelope →
   RETRY, no incident.
5. The extractor sweep matrix: valid → published + source file deleted;
   duplicate (Ignore) → not published but still deleted; validation failure →
   incident routed and file consumed (empty-CloudEvent success, see
   Background); technical failure (fake throws) → file left for the next run.

## Explicit non-deliverables

- **No generic test fixture / pipeline builder.** The "mirror production
  composition, fake the edges" wiring uses each component's own steps and
  contract types; a library version would be hollow or over-parameterized.
  README pattern instead.
- **No component test classes or sample payloads.**
- **No mocking-framework integration or dependency.**
- **No Testcontainers / live-Dapr-sidecar harness.** Whole-system runs with
  real sidecars remain a separate concern.

## Open questions

All resolved:

1. ~~**Repo placement**~~ — this repo: `src/Intropy.Framework.Testing` +
   `test/Intropy.Framework.Testing.Test` in `Intropy.Framework.slnx`. Release
   CI packs everything with the tag version; lockstep is automatic.
2. ~~**`FakeTopic<TCtx>` genericity**~~ — verified against the source:
   `WithSender(SendStep<TCtx>)` uses the builder's own `TCtx`, so the generic
   fake composes for any `TCtx : Context`.
3. ~~**`DeliveryAck` naming**~~ — PascalCase `Success`/`Retry`/`Drop` members;
   wire-value mapping documented in XML docs + README. Unknown/missing acks
   parse to `Retry`.

## Implementation order

1. Project scaffolding (`src` + `test` projects, slnx entries, README stub).
2. `InMemoryFileAdapter` (smallest; validates the package end-to-end).
3. Platform-service fakes (unblocks most consumer tests).
4. `FakeTopic<TCtx>`.
5. `DaprDelivery`.
6. Package self-tests, including analyzer-suppression cleanup.
7. README with the reference usage pattern and both matrices.
8. Template/scaffolder follow-up (separate ticket): scaffolded extractor test
   projects reference the package; scaffolded extractors split sweep
   orchestration from sidecar lifecycle so the orchestration is testable
   without Dapr types.

---

**Changelog from v1:** missing-file semantics → throw (matches production);
`DeleteAsync` missing → no-op; `basePathOverride` → effective-path keying;
business-failure outcome corrected to `Success(defaultValueFactory())` (empty
CloudEvent); fake fault-injection properties typed to
`BusinessIncidentServiceException`/`IdempotencyServiceException`; `NextStatus`
sticky + `QueueStatus` sequencing; `Resolved` list added to the incident fake;
`Depth` → `Count`; thread-safety specified (`lock` + snapshots); unknown
delivery acks → `Retry`; idempotency-service-down row added to the loader
matrix; all three open questions closed.
