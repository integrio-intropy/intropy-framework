using System.Text;
using System.Text.Json;
using Intropy.Framework.Hosting.StatefulSweeps;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.MsSql;

namespace Intropy.Framework.Adapters.Sql.Test;

/// <summary>A SQL source's read of one page with one item, as the adapter makes it.</summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1819:Properties should not return arrays",
    Justification = "The message payload.")]
public sealed record SqlRead(byte[] Content, string? Cursor, IReadOnlyDictionary<string, string> Metadata)
{
    public static async Task<SqlRead> Of(Task<PolledResult> read)
    {
        var pages = Assert.IsType<PolledPages>(await read);
        var all = new List<PolledPage>();
        await foreach (var page in pages.Items)
            all.Add(page);
        var single = Assert.Single(all);
        var item = Assert.Single(single.Items);
        return new SqlRead(item.Content, single.Cursor, item.Metadata);
    }
}

/// <summary>A SQL Server in a container, with the tables the tests read.</summary>
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

    /// <summary>A SQL source on this server, registered the way a component registers it and
    /// resolved as the port's <see cref="IPolledSource"/>.</summary>
    public IPolledSource Source(string query, Delta? delta, SqlEmptyResult emptyResult = SqlEmptyResult.Skip,
        SqlFile? dataAvailable = null, string port = "campaigns")
    {
        var services = new ServiceCollection();
        services.AddSqlSource(port, new SqlSourceOptions
        {
            ConnectionString = ConnectionString, EmptyResult = emptyResult, CursorStore = "statestore",
            InitialWatermark = "2026-01-01T00:00:00"
        }, sql =>
        {
            sql.Query(SqlFile.Inline(query));
            if (delta is not null)
                sql.Delta(delta, dataAvailable);
        });
        return services.BuildServiceProvider().GetRequiredKeyedService<IPolledSource>(port);
    }

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
[Trait("Category", "Integration")]
[Collection(nameof(SqlServerCollection))]
public class SqlSourceIntegrationTests(SqlServerFixture sql)
{
    private const string DeltaQuery =
        "SELECT Id, Name, ModifiedAt FROM dbo.Campaign WHERE ModifiedAt > @watermark ORDER BY ModifiedAt";

    [Fact]
    public async Task AFullExport_ReadsEveryRow()
    {
        var source = Source("SELECT Id, Name FROM dbo.Campaign WHERE Id <= 2 ORDER BY Id", delta: null);

        var read = await SqlRead.Of(source.ReadAsync(CursorRange.All, CancellationToken.None));
        Assert.Null(read.Cursor);
        Assert.Equal("""[{"Id":1,"Name":"Spring"},{"Id":2,"Name":"Summer"}]""", Encoding.UTF8.GetString(read.Content));
        Assert.Equal("2", read.Metadata[SqlContextKeys.RowCount]);
    }

    [Fact]
    public async Task AColumnDelta_ReadsAfterTheWatermark_AndAPinnedRangeIgnoresNewerRows()
    {
        var source = Source(DeltaQuery, Delta.Column("ModifiedAt"), id: "delta");

        var first = await SqlRead.Of(source.ReadAsync(new CursorRange("2026-11-20T01:30:00", null), CancellationToken.None));
        Assert.Equal([2], Ids(first));
        Assert.Equal("datetime:2026-11-20T02:00:00.0000000", first.Cursor);
        Assert.Equal("2026-11-20T02:00:00.0000000", first.Metadata[SqlContextKeys.WatermarkTo]);

        // A row arrives; the pinned re-read of the first range still covers exactly row 2.
        await sql.ExecuteAsync("INSERT INTO dbo.Campaign VALUES (100, N'Autumn', '2026-11-20T03:00:00')");
        var pinned = await SqlRead.Of(source.ReadAsync(new CursorRange("2026-11-20T01:30:00", first.Cursor), CancellationToken.None));
        Assert.Equal([2], Ids(pinned));

        var next = await SqlRead.Of(source.ReadAsync(new CursorRange(first.Cursor, null), CancellationToken.None));
        Assert.Equal([100], Ids(next));
    }

    [Fact]
    public async Task AnEmptyResult_IsSkipped_OrSentAsAnEmptyArray()
    {
        var range = new CursorRange("2099-01-01T00:00:00", null);

        Assert.Same(PolledResult.Empty, await Source(DeltaQuery, Delta.Column("ModifiedAt")).ReadAsync(range, CancellationToken.None));

        var sent = await SqlRead.Of(Source(DeltaQuery, Delta.Column("ModifiedAt"), SqlEmptyResult.Send).ReadAsync(range, CancellationToken.None));
        Assert.Equal("[]", Encoding.UTF8.GetString(sent.Content));
        Assert.Null(sent.Cursor);
    }

    [Fact]
    public async Task ADataAvailableCheck_HoldsTheReadBackUntilTheDataIsReady()
    {
        var source = Source(DeltaQuery, Delta.Column("ModifiedAt"),
            dataAvailable: SqlFile.Inline("SELECT IsReady FROM dbo.Ready WHERE @watermark IS NOT NULL"));
        var range = new CursorRange("2026-11-20T00:00:00", null);

        Assert.Same(PolledResult.NotReady, await source.ReadAsync(range, CancellationToken.None));

        await sql.ExecuteAsync("UPDATE dbo.Ready SET IsReady = 1");
        try
        {
            await SqlRead.Of(source.ReadAsync(range, CancellationToken.None));
        }
        finally
        {
            await sql.ExecuteAsync("UPDATE dbo.Ready SET IsReady = 0");
        }
    }

    [Fact]
    public async Task AnUpToNowDelta_EndsTheReadAtTheDatabasesClockLessTheSettleTime()
    {
        await sql.ExecuteAsync("""
            IF OBJECT_ID('dbo.CampaignChange') IS NOT NULL DROP TABLE dbo.CampaignChange;
            CREATE TABLE dbo.CampaignChange (Id INT NOT NULL PRIMARY KEY, ChangedAt DATETIMEOFFSET NOT NULL);
            INSERT INTO dbo.CampaignChange VALUES
                (1, DATEADD(HOUR, -1, SYSDATETIMEOFFSET())),
                (2, SYSDATETIMEOFFSET());
            """);
        var source = Source("SELECT Id, ChangedAt FROM dbo.CampaignChange WHERE ChangedAt > @watermark ORDER BY ChangedAt",
            Delta.Column("ChangedAt").UpToNow(TimeSpan.FromMinutes(10)));

        var before = DateTimeOffset.UtcNow;
        var read = await SqlRead.Of(source.ReadAsync(
            new CursorRange("datetimeoffset:2026-01-01T00:00:00+00:00", null), CancellationToken.None));
        var after = DateTimeOffset.UtcNow;

        // Row 2 is inside the settle time. The bound is the server's clock less ten minutes, typed as the
        // column is; the container shares the host's clock, so it falls within this read, give or take.
        Assert.Equal([1], Ids(read));
        Assert.StartsWith("datetimeoffset:", read.Cursor, StringComparison.Ordinal);
        var bound = DateTimeOffset.Parse(read.Cursor!["datetimeoffset:".Length..], System.Globalization.CultureInfo.InvariantCulture);
        Assert.InRange(bound, before.AddMinutes(-11), after.AddMinutes(-9));
    }

    private IPolledSource Source(string query, Delta? delta, SqlEmptyResult emptyResult = SqlEmptyResult.Skip,
        SqlFile? dataAvailable = null, string id = "campaigns") =>
        sql.Source(query, delta, emptyResult, dataAvailable, id);

    private static int[] Ids(SqlRead read) =>
        JsonDocument.Parse(read.Content).RootElement.EnumerateArray().Select(row => row.GetProperty("Id").GetInt32()).ToArray();
}

[CollectionDefinition(nameof(SqlServerCollection))]
public class SqlServerCollection : ICollectionFixture<SqlServerFixture>;
