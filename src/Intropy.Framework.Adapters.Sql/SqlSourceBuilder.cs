namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// What a SQL source port reads: its query and, optionally, a delta. Without a delta, the query's
/// complete result is sent on every run.
/// </summary>
public sealed class SqlSourceBuilder
{
    internal SqlFile? QueryFile { get; private set; }
    internal Delta? DeltaMode { get; private set; }
    internal SqlFile? DataAvailableFile { get; private set; }

    /// <summary>The query whose result each run sends, as a JSON array of rows.</summary>
    /// <param name="query">The query, usually <see cref="SqlFile.Embedded(string)"/>.</param>
    /// <returns>The builder, for chaining.</returns>
    public SqlSourceBuilder Query(SqlFile query)
    {
        ArgumentNullException.ThrowIfNull(query);
        QueryFile = query;
        return this;
    }

    /// <summary>
    /// Reads only what changed since the last successful run. The watermark is kept in the port's
    /// <c>CursorStore</c>, set by the system's host, and moves only after the message has been handled.
    /// </summary>
    /// <param name="delta">Where the watermark comes from: <see cref="Sql.Delta.Column"/> or
    /// <see cref="Sql.Delta.Cursor"/>.</param>
    /// <param name="dataAvailable">An optional check run before the query, with <c>@watermark</c>:
    /// a statement returning one value, where <c>NULL</c>, <c>0</c>, <c>false</c> or no row means the
    /// data is not ready yet, and the run sends nothing and keeps the watermark. It is skipped when a
    /// run reads again a range a previous run already read.</param>
    /// <returns>The builder, for chaining.</returns>
    public SqlSourceBuilder Delta(Delta delta, SqlFile? dataAvailable = null)
    {
        ArgumentNullException.ThrowIfNull(delta);
        DeltaMode = delta;
        DataAvailableFile = dataAvailable;
        return this;
    }
}
