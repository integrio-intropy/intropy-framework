# Design: `Intropy.Framework.Testing`

Companion test-helpers package per `docs/plans/testing-package.md`. Hand-rolled
fakes for the four integration-test edges (file adapter, publish topic,
idempotency client, incident client) plus a Dapr CloudEvents delivery helper.

## Project shape

| Item | Decision |
|---|---|
| Project | `src/Intropy.Framework.Testing` — `net10.0`, packable (inherits root `Directory.Build.props`) |
| Test project | `test/Intropy.Framework.Testing.Test` — inherits `test/Directory.Build.props` |
| References | `ProjectReference` → `Intropy.Framework.Blocks` and `Intropy.Framework.Adapters`; `PackageReference` → `Intropy.Contracts` (explicit for clarity; Blocks brings Core + CloudEvents transitively) |
| Solution | Two entries added to `Intropy.Framework.slnx` |
| Versioning | No version property — release CI packs from the git tag; lockstep automatic |

Namespaces mirror the faked edge: `Intropy.Framework.Testing.{Adapters, Topics,
Services, Delivery}`.

## No InternalsVisibleTo (verified)

The implementation requires **no new `InternalsVisibleTo` grants** — a
deliberate deviation from the existing per-package `AssemblyInfo.cs`
convention. Verified against the source:

- Every type the library touches is public: `IFileAdapter`, `SendStep<TCtx>` +
  its abstract `ExecuteAsync`, `TechnicalStepResult<T>.Success(T)`, `Context`,
  the two service clients and their contracts, `JsonEventFormatter`.
- The framework's `InternalsVisibleTo` grants exist because
  `Step.ExecuteAsyncInternal` (the wrapper converting a thrown exception into
  `TechnicalStepResult.Failure` + tracing) is `internal`, and existing test
  projects call it directly.
- Our `FakeTopic` "exception → technical failure" test instead executes the
  fake through the **public** pipeline surface (`Pipeline.Start(...).AddStep(...)`
  in Core), which invokes the internal wrapper from inside Core. This is the
  more representative test anyway: it proves the production path, not a
  test-only backdoor.

## Analyzers (`AnalysisMode=All`, warnings-as-errors)

Suppression strategy: **targeted attributes over `NoWarn`**. Expected hits:
settable `Exception` fault-injection properties and the `DaprDelivery` static
holder — per-member `[SuppressMessage(..., Justification = "...")]` where
needed. The test project inherits the existing test-level `NoWarn` set from
`test/Directory.Build.props`. XML docs on all public API
(`GenerateDocumentationFile=true`).

## 1. `InMemoryFileAdapter` (`Testing.Adapters`)

```csharp
public sealed class InMemoryFileAdapter : IFileAdapter
{
    public InMemoryFileAdapter AddFile(string fileName, string content);   // UTF-8, fluent
    public InMemoryFileAdapter AddFile(string fileName, byte[] content);

    public IReadOnlyDictionary<string, byte[]> Files { get; }  // snapshot, effective-path keys
    public string GetString(string fileName);                  // UTF-8; throws if missing
    public string GetString(string basePath, string fileName); // override-written files

    public Exception? ReadException { get; set; }   // thrown by List/Get; null restores
    public Exception? WriteException { get; set; }  // thrown by Write; null restores
    // IFileAdapter: ListAsync, GetContentAsync x2, WriteAsync x2, DeleteAsync
}
```

- **Storage:** single `Dictionary<string, byte[]>`, key = *effective path* via a
  private `CombinePath` mirroring `LocalFileAdapter.CombinePath` verbatim
  (`string.IsNullOrEmpty(basePath) ? fileName : $"{basePath}/{fileName}"`).
  `StringComparer.OrdinalIgnoreCase`. One `lock` guards all reads/mutations;
  snapshot accessors copy under the lock.
- **Production-matching semantics:** missing `GetContentAsync` (both overloads)
  throws `FileNotFoundException`; the `Encoding` overload delegates to the
  byte[] overload (like `LocalFileAdapter`); `DeleteAsync` on a missing file
  no-ops; `ListAsync` returns everything in the store (documented divergence:
  no configured base path).
- Fault checks happen first in list/get/write.

## 2. `FakeTopic<TCtx>` (`Testing.Topics`)

```csharp
public class FakeTopic<TCtx> : Intropy.Framework.Blocks.Extractor.Steps.SendStep<TCtx>
    where TCtx : Intropy.Framework.Blocks.Shared.Context
{
    public int Count { get; }
    public IReadOnlyList<CloudEvent> Events { get; }  // publication order, snapshot
    public Exception? SendException { get; set; }     // dead-broker simulation
    // ExecuteAsync: SendException set → throw (TechnicalStep catch-all →
    // technical failure, matching a dead broker); else capture + Success(input).
}
```

`StepName` inherited (`"Send"`). Wired via
`ExtractorBuilder<TIn, TOut, TCtx>.WithSender(fakeTopic)` — generic over the
builder's own `TCtx`, composing for any component context.

## 3. Service fakes (`Testing.Services`)

Both use `List<T>` + `lock` internally (not `ConcurrentQueue` — assertion
order matters; extractor sweeps may be parallel), exposed as snapshot
`IReadOnlyList<T>`.

```csharp
public sealed class FakeIdempotencyServiceClient : IIdempotencyServiceClient
{
    public StatusResponse NextStatus { get; set; }        // sticky fallback;
        // default: Proceed / NoPreviousData
    public void QueueStatus(params StatusResponse[] statuses); // dequeued in
        // order while non-empty; then NextStatus
    public IReadOnlyList<MessageInfo> StatusChecks { get; }
    public IReadOnlyList<MessageInfo> Committed { get; }
    public IdempotencyServiceException? StatusException { get; set; }
    // GetInfoAsync serves from an internal commit store; null when uncommitted.
}

public sealed class FakeBusinessIncidentServiceClient : IBusinessIncidentServiceClient
{
    public IReadOnlyList<RecordedIncident> Incidents { get; }
    public IReadOnlyList<ResolvedIncident> Resolved { get; }
    public BusinessIncidentServiceException? TriggerException { get; set; }
    // Trigger records (throws TriggerException first if set). Resolve ALWAYS
    // appends to Resolved and marks the matching incident resolved; unmatched
    // resolve recorded, otherwise no-ops. GetById/List/GetEvents/ResolveManual
    // served from recorded state.
}

public sealed record RecordedIncident(
    Uri Source, string Subject, string Id, BusinessIncidentData Data, string? BatchId);
public sealed record ResolvedIncident(
    Uri Source, string Subject, string Id, string? BatchId);
```

**Typed exceptions are load-bearing:** `ExternalBusinessIncidentRouter` has a
dedicated catch for `BusinessIncidentServiceException`; any other type takes
the generic `Finalizer` catch — a different code path than production.

**Resolved state** lives in an internal mutable projection keyed by
`(Subject, Id)` so `GetById`/`List` reflect resolution while the public
`Incidents` record list stays immutable for assertions.

## 4. `DaprDelivery` (`Testing.Delivery`)

```csharp
public enum DeliveryAck { Success, Retry, Drop }
// PascalCase members ↔ Dapr wire values SUCCESS / RETRY / DROP (XML doc + README)

public static class DaprDelivery
{
    // Structured-mode envelope via JsonEventFormatter.EncodeStructuredModeMessage —
    // same encoding as the framework publisher.
    public static string ToDeliveryEnvelope(CloudEvent cloudEvent);

    // POSTs envelope as application/cloudevents+json; parses {"status":"..."}.
    // Unknown / missing / malformed status → Retry (fail-safe, like the sidecar).
    // Non-2xx → EnsureSuccessStatusCode (surfaces host errors in tests).
    public static Task<DeliveryAck> DeliverAsync(
        this HttpClient client, string route, CloudEvent cloudEvent,
        CancellationToken ct = default);
}
```

## 5. Self-tests (`test/Intropy.Framework.Testing.Test`)

One-test-project-per-package convention (xUnit, `Using Include="Xunit"`, no
NSubstitute needed). Suites:

- **InMemoryFileAdapter**: round-trip; missing → `FileNotFoundException` on both
  overloads; delete-missing no-op; `basePathOverride` effective-path keying;
  fault injection + recovery; sweep scenario consistency.
- **FakeTopic**: capture order/count; `SendException` → technical failure via
  the public `Pipeline.Start(...).AddStep(...)` path (see *No
  InternalsVisibleTo*). `TCtx` = framework `Context`.
- **FakeIdempotency**: default Proceed; sticky `NextStatus`; `QueueStatus`
  sequence then fallback; commit visible via `GetInfoAsync`; typed
  `StatusException`.
- **FakeIncident**: trigger recorded; resolve appends + marks match; unmatched
  resolve no-throw; typed `TriggerException`.
- **DaprDelivery**: envelope round-trip through `JsonEventFormatter`; ack
  matrix SUCCESS/RETRY/DROP/unknown/missing/malformed → Retry (stub
  `HttpMessageHandler`, no ASP.NET dependency).

## 6. README (packed via `Directory.Build.props`)

Canonical pattern: build the component pipeline exactly as production → fake
the edges (`WithSender`, keyed `IFileAdapter` DI override, plain overrides for
the two clients — last registration wins) → assert on fake state. Includes the
loader ack/consumption matrix and extractor sweep matrix from the plan,
including the business-failure subtlety: incident routed = **consumed**
message (`Success(defaultValueFactory())`, empty `CloudEvent`, never null).

## Out of scope

No generic fixture/pipeline builder, no component test classes or payloads, no
mocking-framework integration, no Testcontainers/live-sidecar harness.
