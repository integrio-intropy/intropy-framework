namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// A SQL source port's settings: only what differs between environments. The query and the delta are
/// code (<see cref="SqlSourceBuilder"/>). Bound from the port's section, <c>Ports:&lt;port&gt;</c>.
/// </summary>
public sealed class SqlSourceOptions
{
    /// <summary>The kind every SQL port's section names.</summary>
    public const string SqlKind = "Sql";

    /// <summary>The port's kind: <c>Sql</c>.</summary>
    public string Kind { get; init; } = SqlKind;

    /// <summary>The SQL Server connection string. A secret: never logged. Use a SQL login; Windows
    /// integrated authentication does not work from a Linux container.</summary>
    public string? ConnectionString { get; init; }

    /// <summary>What a run whose query returns no rows sends. Default: nothing.</summary>
    public SqlEmptyResult EmptyResult { get; init; } = SqlEmptyResult.Skip;

    /// <summary>The Dapr state store the watermark is kept in. Required with a delta.</summary>
    public string? WatermarkStore { get; init; }

    /// <summary>The watermark before the first run, such as <c>2026-11-20T01:50:00</c>: a date and
    /// time, an integer, or a <c>0x</c> hex value. Required with a delta.</summary>
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
