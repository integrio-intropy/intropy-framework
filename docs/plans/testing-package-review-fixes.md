# Plan: Intropy.Framework.Testing — review fixes

Follow-up plan addressing findings from a static code review of
`src/Intropy.Framework.Testing` (SOLID analysis, code smells,
simplifications, documentation gaps). Each finding is a self-contained
deliverable with file references; deliverables are ordered by priority and
independent unless noted.

No public API renames are proposed. All changes are additive or
behavior-preserving refactors, except where a deliberate behavior fix is
called out (items 1.1, 1.3).

## Priority 1 — correctness / LSP drift

### 1.1 `InMemoryFileAdapter.ListAsync` does not filter on a base path

`src/Intropy.Framework.Testing/Adapters/InMemoryFileAdapter.cs`

The production `LocalFileAdapter` is configured with a base path and only
lists files under it. The fake lists *every* file in the store, so tests can
pass against the fake and fail against the real adapter (or worse: a test
asserts on files the production sweep would never see).

**Fix:** add an optional constructor parameter `string? basePath = null`.
When set, `ListAsync` returns only keys starting with `basePath + "/"`,
stripped back to the file name (mirroring production). Default `null` keeps
today's behavior, so this is source-compatible. Update the class remarks and
README fake-semantics section.

### 1.2 Fault-injection properties are not thread-safe

Files: all five fakes/captures —
`Adapters/InMemoryFileAdapter.cs` (`ReadException`, `WriteException`,
`DeleteException`), `Services/FakeBusinessIncidentServiceClient.cs`
(`TriggerException`), `Services/FakeIdempotencyServiceClient.cs`
(`NextStatus`, `StatusException`), `Topics/FakeTopic.cs` (`SendException`).

Internal recorded state is lock-guarded, but the fault knobs are plain
mutable auto-properties. Tests that toggle a fault mid-run (the docs
explicitly invite "clearing the property restores normal behavior") race
with the pipeline thread, and the README claims full thread-safety.

**Fix:** mark each fault-injection backing field `volatile` (reference
fields; no tearing, guarantees cross-thread visibility). No locking needed.
Adjust the README "Notes" bullet to state the guarantee precisely:
recorded state is lock-guarded; fault knobs are `volatile` and safe to
toggle between sweeps, not mid-assertion.

### 1.3 `SetDeleteException` creates phantom empty files

`src/Intropy.Framework.Testing/Adapters/InMemoryFileAdapter.cs` (lines
~100–121)

`SetDeleteException(name, ex)` on a file that doesn't exist seeds
`_files[name] = new Entry([])` — a zero-byte file that then appears in
`ListAsync` output and in `Files`. That is a surprising side effect for a
fault-configuration call.

**Fix:** store per-file delete exceptions in a separate
`Dictionary<string, Exception>` instead of on the `Entry`. Deleting a
missing file still no-ops (production semantics); configuring an exception
for a missing file no longer creates one. Also add the symmetric
`SetReadException(string, Exception?)` (and reimplement
`AddUnreadableFile` on top of it) so read/delete fault configuration is
symmetric — `AddUnreadableFile` keeps its "listed but unreadable" seeding
behavior.

### 1.4 Reconcile `GetContentAsync` throw-vs-null contract drift

`src/Intropy.Framework.Adapters/File/IFileAdapter.cs` (docs only) and
`src/Intropy.Framework.Testing/Adapters/InMemoryFileAdapter.cs`

The interface XML docs say "returns null if the file does not exist"; the
fake throws `FileNotFoundException` and claims that matches production (the
Dapr binding throws). One of them is wrong. The byte[] overload's return
type is non-nullable, which already contradicts the doc.

**Fix:** correct the *interface* XML docs in
`Intropy.Framework.Adapters` to state that missing files throw
`FileNotFoundException` (matching the Dapr binding) — the fake and
`LocalFileAdapter` are the source of truth here. Docs-only change; no
behavior change.

## Priority 2 — simplification

### 2.1 Slim `FakeBusinessIncidentServiceClient.Projection`

`src/Intropy.Framework.Testing/Services/FakeBusinessIncidentServiceClient.cs`

`Projection.ToResponse()` hand-copies 14 properties, including
`PreviousId`, `RetryCount`, `LastRetriedAt`, which the fake never mutates
or sets. This is scaffolding that drifts when the contract type changes.

**Fix:** replace the hand-copy with a shallow copy helper (the contract
type is a mutable class; copy via object initializer from the stored
response plus the three fields the projection owns: `Status`, `ResolvedAt`,
`ResolvedBy`). If `IncidentResponse` gains a copy constructor or becomes a
record upstream, use `with` and delete the method. Also promote the
`"manual"` literal in `ResolveManual` to a `const` next to
`ResolvedBySystem`, and add one remark stating that the stored
`Response.Status` is superseded by the computed status after construction.

### 2.2 Simplify `InMemoryFileAdapter.Files` and `GetString` overload

`src/Intropy.Framework.Testing/Adapters/InMemoryFileAdapter.cs`

- `Files`: replace the `Where/Select/new KeyValuePair` chain with
  `ToDictionary(kv => kv.Key, kv => kv.Value.Content!.ToArray(),
  StringComparer.OrdinalIgnoreCase)`.
- `GetString(basePath, fileName)`: delegate to
  `GetString(CombinePath(basePath, fileName))` instead of calling `Read`
  directly, keeping one entry point.

### 2.3 Extract shared event factory in `FakeBusinessIncidentServiceClient.GetEvents`

`src/Intropy.Framework.Testing/Services/FakeBusinessIncidentServiceClient.cs`

The two `new IncidentEventResponse { ... }` blocks share four of five
fields. Extract a local `CreateEvent(Guid incidentId, string type,
DateTimeOffset at)` helper.

### 2.4 Drop redundant `Count` properties

`src/Intropy.Framework.Testing/Dapr/PublishedMessageCapture.cs` and
`src/Intropy.Framework.Testing/Topics/FakeTopic.cs`

`Count` duplicates `Messages.Count` / `Events.Count`. **Decision needed:**
keep for API symmetry across the package, or remove. Default
recommendation: remove both (they are `[EditorBrowsable]`-level sugar and
each is a second lock path to maintain). If any consumer test already uses
`.Count`, keep and mark the duplication in a remark instead.

### 2.5 `PublishedMessage` construction style

`src/Intropy.Framework.Testing/Dapr/PublishedMessage.cs`

The record mixes three positional parameters with a `required`/`init`
fourth. Make `Data` the fourth positional parameter
(`PublishedMessage(string, string, string?, byte[])`), moving the CA1819
suppression to the record parameter. Update `PublishedMessageCapture.Capture`
accordingly. Source-compatible for the package's single call site; breaking
for external consumers constructing the record directly — acceptable for a
pre-1.0 testing package, note it in the changelog.

### 2.6 Allocation-free ack parsing in `DaprDelivery.ParseAck`

`src/Intropy.Framework.Testing/Delivery/DaprDelivery.cs`

Replace `status?.ToUpperInvariant() switch { ... }` with
`string.Equals(status, "SUCCESS", StringComparison.OrdinalIgnoreCase)`
if-chain. Trivial; no behavior change.

### 2.7 Unify the throw-if-set idiom

`InMemoryFileAdapter` has `ThrowIfSet(Exception?)`; the two service fakes
and `FakeTopic` hand-write `if (x is not null) throw x;`. Either share an
internal static helper or hand-write it everywhere consistently.
Recommendation: keep `ThrowIfSet` private per class (no shared internal
surface in a package) but apply it in all four fakes for consistency.

## Priority 3 — documentation

### 3.1 `FakeBusinessIncidentServiceClient.Resolve` — state the match key

The remark says "marks the matching incident resolved". Spell out the
predicate: the *last unresolved projection with equal `Subject` and `CeId`*.

### 3.2 `FakeIdempotencyServiceClient.QueueStatus` — global queue semantics

Document that queued statuses apply across *all* component/id pairs in call
order, not per message. A test interleaving status checks for two messages
consumes the queue in global order.

### 3.3 `FakeTopic<TCtx>.ExecuteAsync` — exception timing and capture skip

Add a remark: when `SendException` is set, the method throws synchronously
and does *not* capture the event — a dead broker does not observe the
publish. Mirrors the note `FakeIdempotencyServiceClient` already has.

### 3.4 `DaprDelivery.DeliverAsync` — routing mistakes surface here

Extend the `<exception>` doc: a wrong route (404) also throws
`HttpRequestException` via `EnsureSuccessStatusCode`, which surfaces test
wiring mistakes as failures rather than retries.

### 3.5 README — label the ack/consumption matrix as framework behavior

`src/Intropy.Framework.Testing/README.md`

The "Loader ack/consumption matrix" describes behavior of the framework's
subscription handler wiring (`Intropy.Framework.Hosting`), not of this
package. Add one sentence labeling it as guidance for tests against
framework-wired loaders, so it doesn't read as a guarantee of this package.

## Verification

- `dotnet build Intropy.Framework.slnx` — zero warnings.
- `dotnet test Intropy.Framework.slnx` — full suite green.
- New tests where behavior changes: `ListAsync` base-path filtering (1.1),
  phantom-entry removal (1.3).
- API-compat check: confirm no consumer in `test/` uses the removed
  `Count` properties (2.4) before removing.
