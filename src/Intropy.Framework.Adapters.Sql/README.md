# Intropy.Framework.Adapters.Sql

A SQL Server source port for the [Intropy.Framework](https://github.com/integrio-intropy/intropy-framework)
pipeline framework: each run of a job reads a query into one message, the result set as a JSON array
of rows, as a full export or a delta with a watermark kept in a Dapr state store.

## Install

```bash
dotnet add package Intropy.Framework.Adapters.Sql
```

## Usage

```csharp
using Intropy.Framework.Adapters.Sql;

services.AddSqlSourcePort("campaigns", configuration, sql => sql
    .Query(SqlFile.Embedded("Sql/Query.sql"))
    .Delta(Delta.Column("MODIFIEDDATETIME")));
```

```json
"Ports": { "campaigns": {
  "Kind": "Sql",
  "ConnectionString": "<secret>",
  "CursorStore": "statestore",
  "InitialWatermark": "2026-11-20T01:50:00"
}}
```

See [SQL Source](https://github.com/integrio-intropy/intropy-framework/blob/main/docs/adapters/sql-source.md)
for the full export and delta behaviour, idempotency, and configuration.

## License

MIT. See the [repository](https://github.com/integrio-intropy/intropy-framework) for source and documentation.
