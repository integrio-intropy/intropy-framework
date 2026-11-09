using System.Data;
using System.Text.Json;

namespace Intropy.Framework.Adapters.Sql.Test;

/// <summary>
/// A result becomes a JSON array of row objects; a column delta keeps only the rows in the watermark
/// range and reports the highest watermark; a cursor delta reports its cursor column's highest value
/// and leaves the column out of the message.
/// </summary>
public class SqlResultWriterTests
{
    private static readonly DateTime T1 = new(2026, 11, 20, 1, 0, 0);
    private static readonly DateTime T2 = new(2026, 11, 20, 2, 0, 0);
    private static readonly DateTime T3 = new(2026, 11, 20, 3, 0, 0);

    [Fact]
    public async Task AFullExport_WritesEveryRow_WithTypedValues()
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Columns.Add("Price", typeof(decimal));
        table.Columns.Add("Active", typeof(bool));
        table.Columns.Add("ModifiedAt", typeof(DateTime));
        table.Columns.Add("Note", typeof(string));
        table.Rows.Add(1, "Spring", 12.50m, true, T1, DBNull.Value);

        var result = await Write(table, delta: null);

        Assert.Equal(1, result.RowCount);
        Assert.Null(result.Watermark);
        Assert.Equal(
            """[{"Id":1,"Name":"Spring","Price":12.50,"Active":true,"ModifiedAt":"2026-11-20T01:00:00","Note":null}]""",
            System.Text.Encoding.UTF8.GetString(result.Content));
    }

    [Fact]
    public async Task AnEmptyResult_IsAnEmptyArray()
    {
        var result = await Write(Rows(), delta: null);

        Assert.Equal(0, result.RowCount);
        Assert.Equal("[]", System.Text.Encoding.UTF8.GetString(result.Content));
    }

    [Fact]
    public async Task AColumnDelta_KeepsRowsAfterFrom_AndReportsTheHighestWatermark()
    {
        var result = await Write(Rows((1, T1), (2, T3), (3, T2)), Delta.Column("modifiedat"), from: T1);

        Assert.Equal([2, 3], Ids(result));
        Assert.Equal(T3, result.Watermark!.Value);
        Assert.Equal(1, result.SkippedRows);
    }

    [Fact]
    public async Task AColumnDelta_WithAPinnedBound_KeepsNoRowBeyondIt()
    {
        var result = await Write(Rows((1, T1), (2, T2), (3, T3)), Delta.Column("ModifiedAt"), from: T1, to: T2);

        Assert.Equal([2], Ids(result));
        Assert.Equal(T2, result.Watermark!.Value);
    }

    [Fact]
    public async Task AColumnDelta_NeverSendsARowWithoutAWatermark()
    {
        var table = Rows((1, T2));
        table.Rows.Add(2, DBNull.Value);

        var result = await Write(table, Delta.Column("ModifiedAt"), from: T1);

        Assert.Equal([1], Ids(result));
    }

    [Fact]
    public async Task AColumnDelta_ComparesWithAnInitialWatermarkGivenAsText()
    {
        var result = await Write(Rows((1, T1), (2, T3)), Delta.Column("ModifiedAt"),
            from: SqlWatermark.Parse("2026-11-20T02:00:00"));

        Assert.Equal([2], Ids(result));
    }

    [Fact]
    public async Task ACursorDelta_ReportsTheCursorColumn_AndLeavesItOutOfTheMessage()
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("__watermark", typeof(long));
        table.Rows.Add(1, 10L);
        table.Rows.Add(2, 12L);

        var result = await Write(table, Delta.Cursor(), from: SqlWatermark.Parse("5"));

        Assert.Equal(12L, result.Watermark!.Value);
        Assert.Equal("""[{"Id":1},{"Id":2}]""", System.Text.Encoding.UTF8.GetString(result.Content));
    }

    [Fact]
    public async Task ADeltaWhoseColumnIsMissing_FailsNamingIt()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Write(Rows((1, T1)), Delta.Column("Changed")));

        Assert.Contains("'Changed'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DuplicateColumnNames_Fail()
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("ID", typeof(int));
        table.Rows.Add(1, 2);

        await Assert.ThrowsAsync<InvalidOperationException>(() => Write(table, delta: null));
    }

    private static DataTable Rows(params (int Id, DateTime ModifiedAt)[] rows)
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("ModifiedAt", typeof(DateTime));
        foreach (var (id, modifiedAt) in rows)
            table.Rows.Add(id, modifiedAt);
        return table;
    }

    private static Task<SqlResultWriter.Result> Write(DataTable table, Delta? delta, DateTime? from = null, DateTime? to = null) =>
        Write(table, delta, from is { } f ? SqlWatermark.FromValue(f, "c") : null, to is { } t ? SqlWatermark.FromValue(t, "c") : null);

    private static async Task<SqlResultWriter.Result> Write(DataTable table, Delta? delta, SqlWatermark? from,
        SqlWatermark? to = null)
    {
        await using var reader = table.CreateDataReader();
        return await SqlResultWriter.WriteAsync(reader, delta, from, to, CancellationToken.None);
    }

    private static int[] Ids(SqlResultWriter.Result result) =>
        JsonDocument.Parse(result.Content).RootElement.EnumerateArray().Select(row => row.GetProperty("Id").GetInt32()).ToArray();
}
