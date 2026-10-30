# Archive

A business incident often ends with the item gone: an extractor deletes the swept file once the
incident is routed, and a loader acknowledges the message. With an *archive store* registered, the
framework copies the payload to the archive before the incident is routed and names the copy on the
incident, so whoever handles it can see what was actually received. In `OnReceipt` mode it also copies
every payload on receipt, so every trace links to its payload.

No pipeline mentions the archive: registering the store and its settings is the whole configuration.

## Registering

```csharp
builder.Services.AddArchiveStore(builder.Configuration);
```

Two configuration sections, like the claim store and its mode:

| Section | Setting | Description |
|---------|---------|-------------|
| `ArchiveStore` | `Kind`, `DaprBindingName`, `BasePath` | The store's adapter: the same settings a port takes, validated the same way |
| `Archive` | `Mode` | `OnIncident` (default), `OnReceipt` or `Never` |
| `Archive` | `BrowseBaseUrl` | Optional. Where a copy can be opened, its key appended — the archive container's address, e.g. `https://stprodarchive001.blob.core.windows.net/archive/` |

```json
"ArchiveStore": { "DaprBindingName": "archive-store" },
"Archive": { "Mode": "OnIncident", "BrowseBaseUrl": "https://stprodarchive001.blob.core.windows.net/archive/" }
```

Without an archive store nothing is archived, whatever the mode, and incidents are raised as before.
`Never` switches archiving off while the store stays registered.

## Modes

| Mode | On receipt | On a business incident |
|------|------------|------------------------|
| `OnIncident` | — | The payload is written before the incident is routed. **Required**: a failed write is a technical failure, so no incident is raised and the item stays — a swept file is kept for the next run, a message is redelivered |
| `OnReceipt` | The payload is written before any step runs, so an item that cannot even be deserialized is archived too. **Best effort**: a failed write is logged and the item is processed without a copy | The copy written on receipt is reused. If that write failed, the payload is written now, and that write is required |
| `Never` | — | — |

Archiving what a component sends is not implemented yet. It will arrive as two new modes, so no
existing value changes meaning on an upgrade: `OnSend` (incidents and every sent payload) and
`Always` (everything received and sent).

Every business incident is archived, retriable ones too: a redelivered message overwrites its own
copy (see the key below), so archiving the attempt that will be retried costs nothing.

## Which pipelines archive

| Block | Archives |
|-------|----------|
| Extractor | The swept file's content |
| Loader | The event's data, after a claim-checked payload is resolved |
| Transactional Integration, send side | The dequeued message, after a claim-checked payload is resolved |
| Transactional Integration, receive side | Nothing: the send side receives exactly the bytes it enqueued, and the receive side cannot raise a business incident (`Enqueue` is a technical step, and its failure keeps the file) |

Every component archives what it received, where its own pipeline runs; a copy's reference never
travels over a queue or a topic. A Transactional Integration whose one send pipeline the caller
registers (`AddSendPipeline`) builds that pipeline itself, so the framework cannot add the archive to
it: declare the send pipeline as a route to have it archive.

The payload is copied unchanged, as bytes: whatever its encoding, the copy is what was received.

## The key

```
{component}/{yyyy}/{MM}/{dd}/{itemId}_{traceId}
```

- **Date** — when the item was received (`received_at`), else the event's time, else now: the first two
  are stable across redeliveries.
- **Item id** — the message id of a Transactional Integration's queued message, the CloudEvent id of a
  loader's event, the file name of a swept file. A character a key cannot hold becomes `_`
  (`order 42.xml` → `order_42.xml`).
- **Trace id** — the trace the copy was written in, so a copy found in the store leads back to its
  trace. Left off without a trace.

A redelivered message continues the trace that travels with it, so it writes to the same key and
overwrites its own copy. A file retried on the next sweep runs in a new trace and keeps a copy per
attempt.

The archive is shared by every component in an environment; the component segment keeps them apart.
The framework never deletes a copy: expire them with a lifecycle policy on the storage.

## Finding a copy

| Where | What |
|-------|------|
| The incident's context | `dataref` (the key) and, with a browse base URL, `dataurl` |
| The trace | An `Archive` span around the write, with `intropy.archive.ref` and `intropy.archive.url`; the same tags on the span of the step that archived. `{ span.intropy.archive.ref != "" }` finds every trace with an archived payload |
| The pipeline context | `archive_ref` (`ArchiveContextKeys.Ref`), for the rest of the pipeline |

## Related

- [Extractor](../blocks/extractor.md), [Loader](../blocks/loader.md),
  [Transactional Integration](../blocks/transactional-integration.md) — the claim check is the archive's
  sibling: the same kind of store, configured the same way
- [Observability](observability.md)
