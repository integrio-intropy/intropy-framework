using System.Text;
using System.Text.Json;
using Intropy.Framework.Hosting.StatefulSweeps;

namespace Intropy.Framework.Adapters.Sql.Test;

/// <summary>
/// What a SQL source makes of a result, read from SQL Server through the public registration: the
/// rows become a JSON array with typed values; a column delta keeps only the rows in the watermark
/// range, never a row without a watermark, and stores its watermark with its type; a cursor delta
/// leaves its cursor column out of the message; a watermark compares with the column's own type
/// (a <c>datetimeoffset</c>, a <c>rowversion</c>); and a result the delta cannot read fails naming the
/// column.
/// </summary>
[Trait("Category", "Integration")]
[Collection(nameof(SqlServerCollection))]
public class SqlResultTests(SqlServerFixture sql)
{
    private const string Rows = """
        SELECT Id, ModifiedAt FROM (VALUES
            (1, CAST('2026-11-20T01:00:00' AS DATETIME2)),
            (2, CAST('2026-11-20T03:00:00' AS DATETIME2)),
            (3, CAST('2026-11-20T02:00:00' AS DATETIME2)),
            (4, CAST(NULL AS DATETIME2))) AS r(Id, ModifiedAt)
        """;

    [Fact]
    public async Task AFullExport_WritesEveryRow_WithTypedValues()
    {
        var source = sql.Source("""
            SELECT CAST(1 AS INT) AS Id, N'Spring' AS Name, CAST(12.50 AS DECIMAL(5,2)) AS Price,
                   CAST(1 AS BIT) AS Active, CAST('2026-11-20T01:00:00' AS DATETIME2) AS ModifiedAt,
                   CAST(NULL AS NVARCHAR(10)) AS Note
            """, delta: null);

        var read = await SqlRead.Of(source.ReadAsync(CursorRange.All, CancellationToken.None));
        Assert.Equal(
            """[{"Id":1,"Name":"Spring","Price":12.50,"Active":true,"ModifiedAt":"2026-11-20T01:00:00","Note":null}]""",
            Encoding.UTF8.GetString(read.Content));
    }

    [Fact]
    public async Task AColumnDelta_KeepsRowsAfterFrom_NeverARowWithoutAWatermark_AndStoresTheHighestWithItsType()
    {
        var read = await SqlRead.Of(sql.Source(Rows, Delta.Column("modifiedat"))
            .ReadAsync(new CursorRange("2026-11-20T01:00:00", null), CancellationToken.None));
        Assert.Equal([2, 3], Ids(read));
        Assert.Equal("datetime:2026-11-20T03:00:00.0000000", read.Cursor);
    }

    [Fact]
    public async Task AColumnDelta_WithAPinnedBound_KeepsNoRowBeyondIt()
    {
        var read = await SqlRead.Of(sql.Source(Rows, Delta.Column("ModifiedAt"))
            .ReadAsync(new CursorRange("2026-11-20T01:00:00", "datetime:2026-11-20T02:00:00.0000000"), CancellationToken.None));
        Assert.Equal([3], Ids(read));
    }

    [Fact]
    public async Task AnInitialWatermarkWithoutAnOffset_ComparesWithADateTimeOffsetColumn()
    {
        var read = await SqlRead.Of(sql.Source("""
            SELECT Id, ModifiedAt FROM (VALUES
                (1, CAST('2026-11-20T01:00:00+00:00' AS DATETIMEOFFSET)),
                (2, CAST('2026-11-20T02:00:00+00:00' AS DATETIMEOFFSET))) AS r(Id, ModifiedAt)
            """, Delta.Column("ModifiedAt")).ReadAsync(new CursorRange("2026-11-20T01:50:00", null), CancellationToken.None));
        Assert.Equal([2], Ids(read));
    }

    [Fact]
    public async Task AnInitialInteger_ComparesWithARowversion()
    {
        var read = await SqlRead.Of(sql.Source("""
            SELECT Id, Version FROM (VALUES
                (1, CAST(0x00000000000007CF AS BINARY(8))),
                (2, CAST(0x00000000000007D1 AS BINARY(8)))) AS r(Id, Version)
            """, Delta.Column("Version")).ReadAsync(new CursorRange("2000", null), CancellationToken.None));
        Assert.Equal([2], Ids(read));
        Assert.StartsWith("binary:0x", read.Cursor, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnUntaggedInitialWithLeadingZeros_IsAString_ComparedAsWritten()
    {
        var read = await SqlRead.Of(sql.Source("""
            SELECT Id, Code FROM (VALUES (1, N'001'), (2, N'002'), (3, N'010')) AS r(Id, Code)
            WHERE Code > @watermark
            """, Delta.Column("Code")).ReadAsync(new CursorRange("001", null), CancellationToken.None));

        Assert.Equal([2, 3], Ids(read));
        Assert.Equal("string:010", read.Cursor);
    }

    [Fact]
    public async Task ATaggedInitial_IsPassedAsItsTagsType()
    {
        // Untagged, "5" is an integer: SQL Server would convert every code to a number, and fail on 'A'.
        var read = await SqlRead.Of(sql.Source("""
            SELECT Id, Code FROM (VALUES (1, N'5'), (2, N'50'), (3, N'6'), (4, N'A')) AS r(Id, Code)
            WHERE Code > @watermark
            """, Delta.Column("Code")).ReadAsync(new CursorRange("string:5", null), CancellationToken.None));

        Assert.Equal([2, 3, 4], Ids(read));
    }

    [Fact]
    public async Task AWatermarkThatCannotBeReadAsTheColumnsType_FailsNamingTheColumn()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sql.Source(Rows, Delta.Column("ModifiedAt"))
            .ReadAsync(new CursorRange("not-a-date", null), CancellationToken.None));

        Assert.Contains("'ModifiedAt'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADeltaWhoseColumnIsMissing_FailsNamingIt()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sql.Source(Rows, Delta.Column("Changed"))
            .ReadAsync(new CursorRange("2026-11-20T01:00:00", null), CancellationToken.None));

        Assert.Contains("'Changed'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DuplicateColumnNames_Fail() =>
        await Assert.ThrowsAsync<InvalidOperationException>(() => sql.Source("SELECT 1 AS Id, 2 AS ID", delta: null)
            .ReadAsync(CursorRange.All, CancellationToken.None));

    private static int[] Ids(SqlRead read) =>
        JsonDocument.Parse(read.Content).RootElement.EnumerateArray().Select(row => row.GetProperty("Id").GetInt32()).ToArray();
}
