# SQL Source

`Intropy.Framework.Adapters.Sql` reads a SQL Server query (on-premises or Azure SQL) as a component's
source port. It is the database counterpart of a file source port: the component runs as a job, and
each run produces **one message**, the query's result set as a JSON array of rows. The message goes
through the component's pipeline exactly like a swept file — into a Transactional Integration's
queue or through an extractor — and a large result goes through the existing claim check.

```bash
dotnet add package Intropy.Framework.Adapters.Sql
```

Only components that read SQL reference this package. It is the only package that references
`Microsoft.Data.SqlClient`; Core, Blocks, Adapters and Hosting stay free of it.

## Registering the port

Each query lives in a `.sql` file in the component, embedded in its assembly, not in configuration:
queries are often long, and their columns must match the component's DTO.

```xml
<ItemGroup>
  <EmbeddedResource Include="Sql/*.sql" />
</ItemGroup>
```

```csharp
using Intropy.Framework.Adapters.Sql;

// Full export: the complete result on every run.
services.AddSqlSourcePort("campaigns", configuration, sql => sql
    .Query(SqlFile.Embedded("Sql/Query.sql")));

// Delta on a column.
services.AddSqlSourcePort("campaigns", configuration, sql => sql
    .Query(SqlFile.Embedded("Sql/Query.sql"))                       // filters on @watermark
    .Delta(Delta.Column("MODIFIEDDATETIME"),
           dataAvailable: SqlFile.Embedded("Sql/DataAvailable.sql"))); // optional pre-check
```

`AddSqlSourcePort` reads `Ports:<port>`, registers the source, and declares the port as the
component's source, like `AddSourcePort(port, configuration)` does for a file port. When the port is
declared elsewhere (a definition's `SourcePort`, or `FileSources.Port` for a component with several
source ports), register only the source:

```csharp
services.AddSqlSource("campaigns", configuration.GetSection("Ports:campaigns"), sql => sql
    .Query(SqlFile.Embedded("Sql/Query.sql")));
```

A port has either a file adapter or a SQL source, never both. Routes, pipelines and the job are
unchanged.

## Configuration

The port's section holds only what differs between environments:

```json
"Ports": {
  "campaigns": {
    "Kind": "Sql",
    "ConnectionString": "<secret>",
    "EmptyResult": "Skip",
    "WatermarkStore": "statestore",
    "InitialWatermark": "2026-11-20T01:50:00",
    "CommandTimeout": "00:05:00"
  }
}
```

| Setting | Meaning |
|---|---|
| `Kind` | `Sql`. |
| `ConnectionString` | SQL Server connection string. Use a SQL login: Windows integrated authentication does not work from a Linux container. |
| `EmptyResult` | `Skip` (default): a run without rows sends nothing. `Send`: it sends an empty array. |
| `WatermarkStore` | The Dapr state store the watermark is kept in. Required with a delta. |
| `InitialWatermark` | The watermark before the first run: a date and time, an integer or a `0x` hex value. Required with a delta. |
| `CommandTimeout` | How long a statement may run. Default five minutes. |

Registration validates the section immediately; unknown settings are rejected, and errors never
include a value (the connection string is a secret). Scheduling, such as once a day at 01:50, is the
CronJob's job, not the adapter's.

## Behaviour

|  | Full export | Delta |
|---|---|---|
| Query | Runs as written | Gets `@watermark` and `@watermarkTo` |
| Watermark | None | Dapr state store, one record per component and port: `committed` and `pending` |
| New watermark | – | `Delta.Column(x)`: highest `x` in the result. `Delta.Cursor()`: highest `__watermark` the query returns. Saved as `pending` before the message is handed over |
| After success | Nothing to complete | `committed = pending`, `pending` cleared |
| After a failure | The next run exports again | `pending` stays, so the next run reads exactly `(committed, pending]` |
| Idempotency | None needed | Key `{port}:{committed}:{pending}` in the context metadata |
| Empty result | `EmptyResult` | `EmptyResult`; the watermark does not move |

The adapter never writes to the database it reads: no timestamp table, no rows marked as sent.

### Delta.Column

The query filters on `@watermark` (`WHERE MODIFIEDDATETIME > @watermark`); the adapter also keeps
only rows in `(@watermark, @watermarkTo]` on that column itself, so the query need not use
`@watermarkTo`, and can be of any shape (CTEs, `ORDER BY`). Rows where the column is `NULL` are never
sent. The column can be a date and time, an integer, a decimal, a `rowversion`/binary or a string
(compared ordinally).

### Delta.Cursor

The query computes the watermark itself and returns it in a column named `__watermark`, which is not
part of the message. The query must read only rows after `@watermark` and, when `@watermarkTo` is set,
none beyond it:

```sql
WHERE Id > @watermark AND (@watermarkTo IS NULL OR Id <= @watermarkTo)
```

### Data available

The optional `dataAvailable` statement runs before the query, with `@watermark`, and returns one
value. `NULL`, `0`, `false` or no row means the data is not ready: the run sends nothing and the
watermark stays. It is skipped when a run reads a pinned range again.

## Why the upper bound is pinned

Delivery is at-least-once, as for files: the message is published and then the watermark is
committed. If the job crashes in between, the next run publishes the same data again, and
idempotency has to absorb it. That only works if the re-read covers exactly the same range — were the
upper bound recomputed, new rows would move it, the key would differ, and the overlapping rows would
be sent twice. So:

1. If `pending` exists, the previous run did not commit: read exactly `(committed, pending]`.
2. Otherwise read `(committed, …]` and compute the new watermark.
3. Save `pending`, then hand over the message with the key `{port}:{committed}:{pending}`.
4. Once it is handled, save `committed = pending` and clear `pending`.

Use the key with `WithSourceIdempotency`, which reads it from the context metadata instead of the
payload:

```csharp
// The send side of a Transactional Integration
SendPipelineBuilder<Campaigns, Export, Context>.Create(name, options, loggerFactory)
    .WithSourceIdempotency(idempotencyClient)
    ...

// An extractor
ExtractorBuilder<Campaigns, Event, Context>.Create(name, provider)
    .WithSourceIdempotency()
    ...
```

The id is the range, the date is when the source was read, and the hash is constant, so the range
alone decides. A full export sets no key and needs no idempotency.

## Context and payload

The message's context metadata has `source_port` and `received_at` (as for files, but no
`file_name`), `sql.rowCount`, and with a delta `sql.watermark.from` and `sql.watermark.to`
(`SqlContextKeys`). The payload is the rows as a JSON array (`byte[]`), one object per row and one
property per column; the component's deserialize step maps it to its own DTO. Every column needs a
distinct name: alias computed columns.

## The shared part: StatefulSweep

The watermark protocol is not SQL-specific. It lives in `Intropy.Framework.Hosting.StatefulSweeps`,
and any source that keeps a cursor outside the system it reads (an HTTP poll with `since`/`until`, an
ETag, a page token) can use it:

- **`IPolledSource`** — the source. It only reads: given `(from, to]` it returns the payload and the
  new cursor. Register it keyed by the port name, like a file adapter.
- **`StatefulSweep`** — owns the protocol: reads `committed`/`pending`, asks the source, saves
  `pending`, hands the message over with the idempotency key, then commits.
- **`ICursorStore`** — where the cursor is kept; by default `DaprCursorStore` on the source's state
  store. The cursor is an opaque string.
- **`CursorOptions.CanPin`** — a source that cannot read a bounded range again (a one-time
  continuation token) sets it to `false`: no `pending` is kept, and delivery is plain at-least-once.

The sweep assumes one run of a component at a time, as a scheduled job has.

## See also

- [File Adapters](file-adapters.md)
- [Transactional Integration](../blocks/transactional-integration.md)
- [Extractor](../blocks/extractor.md)
