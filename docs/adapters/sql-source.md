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
component's single source port (a `PolledSourcePort`), like `AddSourcePort(port, configuration)`
does for a file port. When the port is declared elsewhere (a definition's `Source`, or the sources
of a component with routes), register only the source and declare the port as a
`PolledSourcePort`:

```csharp
services.AddSqlSource("campaigns", configuration.GetSection("Ports:campaigns"), sql => sql
    .Query(SqlFile.Embedded("Sql/Query.sql")));
services.AddExtractor(definition,
    sources => sources.Port(new PolledSourcePort("campaigns")),
    routes => routes.On<Campaigns, CampaignsOut>(["campaigns"], ConfigurePipeline));
```

A port declared by name alone (`sources.Port("campaigns")`, a definition's `SourcePort`) is a file
port: declaring a SQL port that way fails the run's validation, naming the fix. A port has either a
file adapter or a SQL source, never both. Routes, pipelines and the job are unchanged.

## Configuration

The port's section holds only what differs between environments:

```json
"Ports": {
  "campaigns": {
    "Kind": "Sql",
    "ConnectionString": "<secret>",
    "EmptyResult": "Skip",
    "InitialWatermark": "2026-11-20T01:50:00",
    "CommandTimeout": "00:05:00"
  }
}
```

| Setting | Meaning |
|---|---|
| `Kind` | `Sql` (SQL Server), or `Sqlite` for a local run. See [SQLite for local runs](#sqlite-for-local-runs). |
| `ConnectionString` | SQL Server connection string. Use a SQL login: Windows integrated authentication does not work from a Linux container. |
| `EmptyResult` | `Skip` (default): a run without rows sends nothing. `Send`: it sends an empty array. |
| `CursorStore` | The Dapr state store the watermark is kept in. Required with a delta. Set by the system's host; see below. |
| `InitialWatermark` | The watermark before the first run. Required with a delta. See [The initial watermark](#the-initial-watermark). |
| `CommandTimeout` | How long a statement may run. Default five minutes. |

Declare the port with `PortRef.DefineExternal` in the system's topology. The topology mints a state
store for each component that reads an external port (`<component>-cursors`) and the host passes its
name as `Ports__<port>__CursorStore`, so a deployed component never names its store. Set
`CursorStore` yourself only when the component runs outside the host. A full export ignores it.

**The watermark is this source's cursor.** Every polled source port is driven by a stateful sweep
that keeps the source's position between runs, which the framework calls its *cursor*, in the
`CursorStore`. For a SQL source that position is a watermark: the highest value of the watermark
column read so far (`int:2`, a date and time, a `rowversion`), which the next run's query receives as
`@watermark`. `InitialWatermark` is the cursor before the first run. The rest of this page says
watermark.

### The initial watermark

The initial watermark is the first run's `@watermark`, so its type is the parameter's SQL type.
Tag it with the watermark column's type to say which:

| Tag | Example | SQL type |
|---|---|---|
| `datetime:` | `datetime:2026-11-20T01:50:00` | `datetime2` |
| `datetimeoffset:` | `datetimeoffset:2026-11-20T01:50:00+01:00` | `datetimeoffset` |
| `int:` | `int:1000` | `bigint` |
| `decimal:` | `decimal:12.5` | `decimal` |
| `binary:` | `binary:0x00000000000007D0` | `varbinary` (a `rowversion`) |
| `string:` | `string:001` | `nvarchar` |

Untagged, the type is inferred: an integer written without leading zeros or sign (`1000`), a `0x`
hex value, an ISO 8601 date and time (with an offset, a `datetimeoffset`), or else a string. Tag it
whenever that could guess wrong: on a string column, `5` would be passed as a number and SQL Server
would convert the column's values to compare. Registration rejects a value that does not match its
tag. Later watermarks are stored with their tag, taken from the column.

Registration validates the section immediately; unknown settings are rejected, and errors never
include a value (the connection string is a secret). Scheduling, such as once a day at 01:50, is the
CronJob's job, not the adapter's.

### SQLite for local runs

`Kind: Sqlite` reads a SQLite database file instead of SQL Server, with the same query file and
delta: a local run needs no database server. The environment chooses the kind, so a component's
`appsettings.json` can say `Sql` while the local system host sets `Ports__<port>__Kind=Sqlite` and a
connection string such as `Data Source=/abs/path/marketing.db`. In the system's development
definition:

```csharp
development.Settings(Ports.Campaigns)
    .Set("Kind", "Sqlite")
    .Set("ConnectionString", "Data Source=/abs/path/marketing.db");
```

The query runs unchanged on both, so keep it to SQL both accept: a plain `SELECT … WHERE column >
@watermark ORDER BY column` does; `TOP`, T-SQL functions and SQL Server types do not. SQLite has no
declared types: use an integer or a text watermark column, and a date and time watermark is passed
as ISO 8601 text.

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
watermark stays (the source reports `PolledResult.NotReady`, which never commits a pending range). It is skipped when a run reads a pinned range again.

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

## The shared part: polled source ports

The watermark protocol is not SQL-specific. It lives in `Intropy.Framework.Hosting.StatefulSweeps`,
and any source that keeps a cursor outside the system it reads (an HTTP API with `since`/`until`, an
ETag, a page token, a feed's archive pages) can use it:

- **`IPolledSource`** — the source. It only reads: given `(from, to]` it returns pages of items,
  each page with the cursor after it. Register it keyed by the port name (`AddPolledSource`), like
  a file adapter. The SQL source is one, reading one page with one item: the rows.
- **`PolledSourcePort`** — the kind of source port whose sweep owns the protocol: reads
  `committed`/`pending`, pins each page as `pending`, hands its items over in order with their
  idempotency keys, commits the page with its last item, and stops at the first failed item.
- **`ICursorStore`** — where the cursor is kept; by default `DaprCursorStore` on the source's state
  store, saved first-write-wins, so a second run fails instead of moving the watermark too. The
  cursor is an opaque string. `InMemoryCursorStore` (Intropy.Framework.Testing) is its fake.
- **`CursorOptions.CanPin`** — a source that cannot read a bounded range again (a one-time
  continuation token) sets it to `false`: no `pending` is kept, and delivery is plain at-least-once.

## See also

- [File Adapters](file-adapters.md)
- [Transactional Integration](../blocks/transactional-integration.md)
- [Extractor](../blocks/extractor.md)
