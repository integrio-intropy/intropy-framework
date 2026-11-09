using System.Text;
using System.Text.Json;
using Intropy.Framework.Hosting.StatefulSweeps;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.MsSql;

namespace Intropy.Framework.Adapters.Sql.Test;

/// <summary>A SQL Server in a container, with one table the tests read.</summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04").Build();

    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        await ExecuteAsync("""
            CREATE TABLE dbo.Campaign (Id INT NOT NULL PRIMARY KEY, Name NVARCHAR(50) NOT NULL, ModifiedAt DATETIME NOT NULL);
            INSERT INTO dbo.Campaign VALUES (1, N'Spring', '2026-11-20T01:00:00'), (2, N'Summer', '2026-11-20T02:00:00');
            CREATE TABLE dbo.Ready (IsReady BIT NOT NULL);
            INSERT INTO dbo.Ready VALUES (0);
            """);
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100", Justification = "Test setup statements.")]
    public async Task ExecuteAsync(string sql)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}

/// <summary>
/// The SQL source against SQL Server: a full export reads every row; a column delta passes typed
/// watermark parameters, reads only rows after the watermark, re-reads exactly a pinned range even
/// after new rows arrive, and honours a data-available check; an empty result is skipped or sent as
/// an empty array. The source never writes to the database.
/// </summary>
[Collection(nameof(SqlServerCollection))]
public class SqlSourceIntegrationTests(SqlServerFixture sql)
{
    private const string DeltaQuery =
        "SELECT Id, Name, ModifiedAt FROM dbo.Campaign WHERE ModifiedAt > @watermark ORDER BY ModifiedAt";

    [Fact]
    public async Task AFullExport_ReadsEveryRow()
    {
        var source = Source("SELECT Id, Name FROM dbo.Campaign WHERE Id <= 2 ORDER BY Id", delta: null);

        var read = await source.ReadAsync(CursorRange.All, CancellationToken.None);

        Assert.NotNull(read);
        Assert.Null(read.Cursor);
        Assert.Equal("""[{"Id":1,"Name":"Spring"},{"Id":2,"Name":"Summer"}]""", Encoding.UTF8.GetString(read.Content));
        Assert.Equal("2", read.Metadata[SqlContextKeys.RowCount]);
    }

    [Fact]
    public async Task AColumnDelta_ReadsAfterTheWatermark_AndAPinnedRangeIgnoresNewerRows()
    {
        var source = Source(DeltaQuery, Delta.Column("ModifiedAt"), id: "delta");

        var first = await source.ReadAsync(new CursorRange("2026-11-20T01:30:00", null), CancellationToken.None);
        Assert.NotNull(first);
        Assert.Equal([2], Ids(first));
        Assert.Equal("datetime:2026-11-20T02:00:00.0000000", first.Cursor);
        Assert.Equal("2026-11-20T02:00:00.0000000", first.Metadata[SqlContextKeys.WatermarkTo]);

        // A row arrives; the pinned re-read of the first range still covers exactly row 2.
        await sql.ExecuteAsync("INSERT INTO dbo.Campaign VALUES (100, N'Autumn', '2026-11-20T03:00:00')");
        var pinned = await source.ReadAsync(new CursorRange("2026-11-20T01:30:00", first.Cursor), CancellationToken.None);
        Assert.NotNull(pinned);
        Assert.Equal([2], Ids(pinned));

        var next = await source.ReadAsync(new CursorRange(first.Cursor, null), CancellationToken.None);
        Assert.NotNull(next);
        Assert.Equal([100], Ids(next));
    }

    [Fact]
    public async Task AnEmptyResult_IsSkipped_OrSentAsAnEmptyArray()
    {
        var range = new CursorRange("2099-01-01T00:00:00", null);

        Assert.Null(await Source(DeltaQuery, Delta.Column("ModifiedAt")).ReadAsync(range, CancellationToken.None));

        var sent = await Source(DeltaQuery, Delta.Column("ModifiedAt"), SqlEmptyResult.Send).ReadAsync(range, CancellationToken.None);
        Assert.NotNull(sent);
        Assert.Equal("[]", Encoding.UTF8.GetString(sent.Content));
        Assert.Null(sent.Cursor);
    }

    [Fact]
    public async Task ADataAvailableCheck_HoldsTheReadBackUntilTheDataIsReady()
    {
        var source = Source(DeltaQuery, Delta.Column("ModifiedAt"),
            dataAvailable: SqlFile.Inline("SELECT IsReady FROM dbo.Ready WHERE @watermark IS NOT NULL"));
        var range = new CursorRange("2026-11-20T00:00:00", null);

        Assert.Null(await source.ReadAsync(range, CancellationToken.None));

        await sql.ExecuteAsync("UPDATE dbo.Ready SET IsReady = 1");
        try
        {
            Assert.NotNull(await source.ReadAsync(range, CancellationToken.None));
        }
        finally
        {
            await sql.ExecuteAsync("UPDATE dbo.Ready SET IsReady = 0");
        }
    }

    [Fact]
    public async Task ACursorDelta_GetsBothBounds_AndReportsItsCursor()
    {
        var source = Source("""
            SELECT Id, CAST(Id AS BIGINT) AS __watermark FROM dbo.Campaign
            WHERE Id > @watermark AND (@watermarkTo IS NULL OR Id <= @watermarkTo) AND Id <= 2
            """, Delta.Cursor());

        var read = await source.ReadAsync(new CursorRange("0", "1"), CancellationToken.None);

        Assert.NotNull(read);
        Assert.Equal("""[{"Id":1}]""", Encoding.UTF8.GetString(read.Content));
        Assert.Equal("int:1", read.Cursor);
    }

    private SqlSource Source(string query, Delta? delta, SqlEmptyResult emptyResult = SqlEmptyResult.Skip,
        SqlFile? dataAvailable = null, string id = "campaigns") =>
        new(id, new SqlSourceOptions
            {
                ConnectionString = sql.ConnectionString, EmptyResult = emptyResult, WatermarkStore = "statestore",
                InitialWatermark = "2026-01-01T00:00:00"
            },
            SqlFile.Inline(query), delta, dataAvailable, NullLogger<SqlSource>.Instance);

    private static int[] Ids(PolledRead read) =>
        JsonDocument.Parse(read.Content).RootElement.EnumerateArray().Select(row => row.GetProperty("Id").GetInt32()).ToArray();
}

[CollectionDefinition(nameof(SqlServerCollection))]
public class SqlServerCollection : ICollectionFixture<SqlServerFixture>;
