using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Intropy.Framework.Hosting.StatefulSweeps;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// A SQL Server query as a polled source port: each run reads the query's result into one page with
/// one item, the rows as a JSON array. Without a delta it reads the complete result (a full export); with one, it
/// reads the rows after the committed watermark, and can read a pinned range again.
/// </summary>
/// <remarks>
/// The source only reads: it never writes to the database it reads (no timestamp table, no rows
/// marked as sent). The watermark is kept and moved by the sweep.
/// </remarks>
internal sealed class SqlSource(
    string port,
    SqlSourceOptions options,
    SqlFile query,
    Delta? delta,
    SqlFile? dataAvailable,
    ILogger<SqlSource> logger) : IPolledSource
{
    internal const string WatermarkParameter = "@watermark";
    internal const string WatermarkToParameter = "@watermarkTo";

    /// <inheritdoc />
    public CursorOptions? Cursor { get; } = delta is null
        ? null
        : new CursorOptions(options.CursorStore!, options.InitialWatermark, CanPin: true);

    /// <inheritdoc />
    public async Task<PolledResult> ReadAsync(CursorRange range, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(range);
        var from = range.From is null ? null : SqlWatermark.Parse(range.From);
        var to = range.To is null ? null : SqlWatermark.Parse(range.To);

        await using DbConnection connection = options.IsSqlite
            ? new SqliteConnection(options.ConnectionString)
            : new SqlConnection(options.ConnectionString);
        await connection.OpenAsync(ct);

        if (delta is not null && dataAvailable is not null && to is null
            && !await IsDataAvailableAsync(connection, from, ct))
        {
            logger.LogInformation("The data of source port {Port} is not available yet (watermark {From}); nothing is read",
                port, from);
            return PolledResult.NotReady;
        }

        SqlResultWriter.Result result;
        await using (var command = Command(connection, query, from, to))
        await using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult, ct))
            result = await SqlResultWriter.WriteAsync(reader, delta, from, to, ct);

        if (result.SkippedRows > 0)
            logger.LogInformation(
                "Source port {Port} left out {Skipped} row(s) outside the watermark range ({From}, {To}] or without a watermark",
                port, result.SkippedRows, from, to);

        if (result.RowCount == 0 && options.EmptyResult == SqlEmptyResult.Skip)
        {
            logger.LogInformation("The query of source port {Port} returned no rows; nothing is sent", port);
            return PolledResult.Empty;
        }

        logger.LogInformation("Read {Rows} row(s) from source port {Port}", result.RowCount, port);
        var metadata = new Dictionary<string, string>
        {
            [SqlContextKeys.RowCount] = result.RowCount.ToString(CultureInfo.InvariantCulture)
        };
        if (delta is not null)
        {
            metadata[SqlContextKeys.WatermarkFrom] = from?.Text ?? "";
            metadata[SqlContextKeys.WatermarkTo] = (to ?? result.Watermark ?? from)?.Text ?? "";
        }

        // The whole result is one page with one item: the rows, as a JSON array.
        return PolledResult.Page([new PolledItem(result.Content) { Metadata = metadata }], result.Watermark?.Stored);
    }

    private async Task<bool> IsDataAvailableAsync(DbConnection connection, SqlWatermark? from, CancellationToken ct)
    {
        await using var command = Command(connection, dataAvailable!, from, null);
        return await command.ExecuteScalarAsync(ct) switch
        {
            null or DBNull => false,
            bool v => v,
            string v => v.Length > 0 && v != "0" && !string.Equals(v, "false", StringComparison.OrdinalIgnoreCase),
            IConvertible v => Convert.ToDecimal(v, CultureInfo.InvariantCulture) != 0,
            _ => true
        };
    }

    [SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities",
        Justification = "The statement is the component's own SQL, embedded in its code; values are parameters.")]
    private DbCommand Command(DbConnection connection, SqlFile statement, SqlWatermark? from, SqlWatermark? to)
    {
        var command = connection.CreateCommand();
        command.CommandText = statement.Text;
        command.CommandTimeout = (int)Math.Ceiling(options.CommandTimeout.TotalSeconds);
        if (delta is null)
            return command;

        var type = from ?? to;
        command.Parameters.Add(SqlWatermark.Parameter(command, WatermarkParameter, from, type));
        command.Parameters.Add(SqlWatermark.Parameter(command, WatermarkToParameter, to, type));
        return command;
    }
}
