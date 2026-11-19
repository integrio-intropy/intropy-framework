using System.Buffers;
using System.Data.Common;
using System.Globalization;
using System.Text.Json;

namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// Writes a query's result as a JSON array of row objects, one property per column, and finds the
/// watermark after it.
/// </summary>
internal static class SqlResultWriter
{
    /// <summary>The written result.</summary>
    /// <param name="Content">The rows as a UTF-8 JSON array.</param>
    /// <param name="RowCount">The rows written.</param>
    /// <param name="Watermark">The highest watermark of the rows written, or <see langword="null"/>
    /// without a delta or without rows.</param>
    /// <param name="SkippedRows">Rows a delta left out: outside the range, or with a <c>NULL</c>
    /// watermark.</param>
    internal sealed record Result(byte[] Content, int RowCount, SqlWatermark? Watermark, int SkippedRows);

    /// <summary>Reads every row of <paramref name="reader"/>'s first result set.</summary>
    /// <param name="reader">The result.</param>
    /// <param name="delta">The delta, or <see langword="null"/> for a full export.</param>
    /// <param name="from">The committed watermark: a column delta keeps rows after it.</param>
    /// <param name="to">The pinned upper bound: a column delta keeps no row beyond it.</param>
    /// <param name="ct">A cancellation token.</param>
    /// <exception cref="InvalidOperationException">A column has no name or a duplicate name, the
    /// delta's column is missing, or a watermark value cannot be compared.</exception>
    internal static async Task<Result> WriteAsync(DbDataReader reader, Delta? delta, SqlWatermark? from,
        SqlWatermark? to, CancellationToken ct)
    {
        var columns = Columns(reader);
        var watermarkOrdinal = -1;
        if (delta is not null)
        {
            watermarkOrdinal = Array.FindIndex(columns,
                c => string.Equals(c, delta.WatermarkColumn, StringComparison.OrdinalIgnoreCase));
            if (watermarkOrdinal < 0)
                throw new InvalidOperationException(
                    $"The query's result has no column '{delta.WatermarkColumn}', which the delta ({delta}) reads " +
                    "the watermark from.");
        }

        var buffer = new ArrayBufferWriter<byte>();
        var rows = 0;
        var skipped = 0;
        SqlWatermark? highest = null;
        await using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartArray();
            while (await reader.ReadAsync(ct))
            {
                if (delta is not null)
                {
                    var keep = Watermark(reader, watermarkOrdinal, delta, from, to, ref highest);
                    if (!keep)
                    {
                        skipped++;
                        continue;
                    }
                }

                json.WriteStartObject();
                for (var i = 0; i < columns.Length; i++)
                {
                    json.WritePropertyName(columns[i]);
                    WriteValue(json, reader, i);
                }

                json.WriteEndObject();
                rows++;
            }

            json.WriteEndArray();
        }

        return new Result(buffer.WrittenSpan.ToArray(), rows, highest, skipped);
    }

    /// <returns>Whether the row is kept: a delta keeps rows in <c>(from, to]</c> with a watermark.</returns>
    private static bool Watermark(DbDataReader reader, int ordinal, Delta delta, SqlWatermark? from,
        SqlWatermark? to, ref SqlWatermark? highest)
    {
        if (reader.IsDBNull(ordinal))
            return false;

        var value = SqlWatermark.FromValue(reader.GetValue(ordinal), delta.WatermarkColumn);
        // A read up to now compares dates as instants, also SQLite's, which are text.
        if (delta.Settle is not null)
            value = value.AsDateTime(delta.WatermarkColumn);
        if (from is not null && value.CompareTo(from.As(value, delta.WatermarkColumn)) <= 0)
            return false;
        if (to is not null && value.CompareTo(to.As(value, delta.WatermarkColumn)) > 0)
            return false;

        if (highest is null || value.CompareTo(highest.As(value, delta.WatermarkColumn)) > 0)
            highest = value;
        return true;
    }

    private static string[] Columns(DbDataReader reader)
    {
        var columns = new string[reader.FieldCount];
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < columns.Length; i++)
        {
            var name = reader.GetName(i);
            if (string.IsNullOrWhiteSpace(name))
                throw new InvalidOperationException(
                    $"Column {i + 1} of the query's result has no name; give every computed column an alias (AS Name).");
            if (!seen.Add(name))
                throw new InvalidOperationException(
                    $"The query's result has more than one column named '{name}'; each column becomes a property of " +
                    "the row, so give each a distinct alias.");
            columns[i] = name;
        }

        return columns;
    }

    private static void WriteValue(Utf8JsonWriter json, DbDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            json.WriteNullValue();
            return;
        }

        switch (reader.GetValue(ordinal))
        {
            case string v: json.WriteStringValue(v); break;
            case bool v: json.WriteBooleanValue(v); break;
            case byte v: json.WriteNumberValue(v); break;
            case short v: json.WriteNumberValue(v); break;
            case int v: json.WriteNumberValue(v); break;
            case long v: json.WriteNumberValue(v); break;
            case decimal v: json.WriteNumberValue(v); break;
            case double v: json.WriteNumberValue(v); break;
            case float v: json.WriteNumberValue(v); break;
            case DateTime v: json.WriteStringValue(v); break;
            case DateTimeOffset v: json.WriteStringValue(v); break;
            case TimeSpan v: json.WriteStringValue(v.ToString("c", CultureInfo.InvariantCulture)); break;
            case Guid v: json.WriteStringValue(v); break;
            case byte[] v: json.WriteBase64StringValue(v); break;
            case var v: json.WriteStringValue(Convert.ToString(v, CultureInfo.InvariantCulture)); break;
        }
    }
}
