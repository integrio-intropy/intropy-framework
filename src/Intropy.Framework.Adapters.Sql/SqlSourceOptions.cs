using Intropy.Framework.Hosting.StatefulSweeps;

namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// A SQL source port's settings: only what differs between environments. The query and the delta are
/// code (<see cref="SqlSourceBuilder"/>). Bound from the port's section, <c>Ports:&lt;port&gt;</c>.
/// </summary>
public sealed class SqlSourceOptions
{
    /// <summary>The kind of a SQL Server port.</summary>
    public const string SqlKind = "Sql";

    /// <summary>The kind of a SQLite port: a local stand-in for SQL Server, a database file with no
    /// server to run. The same query runs against it, so keep the query to SQL both accept.</summary>
    public const string SqliteKind = "Sqlite";

    /// <summary>The port's kind: <c>Sql</c> (SQL Server) or <c>Sqlite</c>. The environment chooses it,
    /// like a file port's binding type: a local run can read SQLite where the deployment reads SQL
    /// Server.</summary>
    public string Kind { get; init; } = SqlKind;

    /// <summary>Whether the port reads SQLite rather than SQL Server.</summary>
    internal bool IsSqlite => string.Equals(Kind?.Trim(), SqliteKind, StringComparison.OrdinalIgnoreCase);

    /// <summary>The SQL Server (or SQLite) connection string. A secret: never logged. Use a SQL login; Windows
    /// integrated authentication does not work from a Linux container.</summary>
    public string? ConnectionString { get; init; }

    /// <summary>What a run whose query returns no rows sends. Default: nothing.</summary>
    public SqlEmptyResult EmptyResult { get; init; } = SqlEmptyResult.Skip;

    /// <summary>The Dapr state store the watermark is kept in (<see cref="CursorOptions.StoreKey"/>).
    /// Required with a delta. The system's host sets it from the topology, which mints a store for every
    /// component that reads an external port; set it yourself only outside the host.</summary>
    public string? CursorStore { get; init; }

    /// <summary>The watermark before the first run, such as <c>2026-11-20T01:50:00</c>: a date and
    /// time, an integer, or a <c>0x</c> hex value. Required with a delta. The watermark is this
    /// source's cursor, kept in <see cref="CursorStore"/>: this is the cursor's value before any run
    /// has committed one.</summary>
    public string? InitialWatermark { get; init; }

    /// <summary>How long a statement may run before it fails. Default: five minutes.</summary>
    public TimeSpan CommandTimeout { get; init; } = TimeSpan.FromMinutes(5);
}

/// <summary>
/// What a SQL source sends when its query returns no rows.
/// </summary>
public enum SqlEmptyResult
{
    /// <summary>Nothing: the run succeeds without a message.</summary>
    Skip,

    /// <summary>A message with an empty array, so the receiving side knows the run happened.</summary>
    Send
}
