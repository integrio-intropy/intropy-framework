using System.Text;
using System.Text.Json;
using Intropy.Framework.Hosting.StatefulSweeps;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Adapters.Sql.Test;

/// <summary>
/// The SQL source against SQLite, the local stand-in for SQL Server: the same registration with
/// <c>Kind: Sqlite</c> reads a database file. A column delta passes the watermark, reads only rows
/// after it, re-reads exactly a pinned range, and keeps an integer watermark typed.
/// </summary>
public sealed class SqliteSourceTests : IDisposable
{
    private const string DeltaQuery =
        "SELECT ChangeId, CampaignId, ChangedAt FROM CampaignChange WHERE ChangeId > @watermark ORDER BY ChangeId";

    private readonly string _path = Path.Combine(Path.GetTempPath(), $"intropy-sqlite-{Guid.NewGuid():N}.db");

    public SqliteSourceTests() => Execute("""
        CREATE TABLE CampaignChange (ChangeId INTEGER PRIMARY KEY, CampaignId TEXT NOT NULL, ChangedAt TEXT NOT NULL);
        INSERT INTO CampaignChange VALUES (1, 'spring', '2026-03-01T08:00:00+00:00'), (2, 'summer', '2026-06-01T08:00:00+00:00');
        """);

    private string ConnectionString => $"Data Source={_path};Pooling=False";

    public void Dispose() => System.IO.File.Delete(_path);

    [Fact]
    public async Task AFullExport_ReadsEveryRow()
    {
        var source = Source("SELECT ChangeId, CampaignId FROM CampaignChange ORDER BY ChangeId", delta: null);

        var read = await SqlRead.Of(source.ReadAsync(CursorRange.All, CancellationToken.None));
        Assert.Null(read.Cursor);
        Assert.Equal("""[{"ChangeId":1,"CampaignId":"spring"},{"ChangeId":2,"CampaignId":"summer"}]""",
            Encoding.UTF8.GetString(read.Content));
    }

    [Fact]
    public async Task AColumnDelta_ReadsAfterTheWatermark_AndAPinnedRangeIgnoresNewerRows()
    {
        var source = Source(DeltaQuery, Delta.Column("ChangeId"));

        var first = await SqlRead.Of(source.ReadAsync(new CursorRange("int:0", null), CancellationToken.None));
        Assert.Equal([1, 2], Ids(first));
        Assert.Equal("int:2", first.Cursor);

        // A row arrives; the pinned re-read of the first range still covers exactly rows 1 and 2.
        Execute("INSERT INTO CampaignChange VALUES (3, 'autumn', '2026-09-01T08:00:00+00:00')");
        var pinned = await SqlRead.Of(source.ReadAsync(new CursorRange("int:0", first.Cursor), CancellationToken.None));
        Assert.Equal([1, 2], Ids(pinned));

        var next = await SqlRead.Of(source.ReadAsync(new CursorRange(first.Cursor, null), CancellationToken.None));
        Assert.Equal([3], Ids(next));
        Assert.Equal("int:3", next.Cursor);
    }

    [Fact]
    public async Task ATextColumn_IsSentAsWritten()
    {
        var source = Source(DeltaQuery, Delta.Column("ChangeId"));

        var read = await SqlRead.Of(source.ReadAsync(new CursorRange("int:1", null), CancellationToken.None));
        var row = Assert.Single(JsonDocument.Parse(read.Content).RootElement.EnumerateArray());
        Assert.Equal("2026-06-01T08:00:00+00:00", row.GetProperty("ChangedAt").GetString());
    }

    [Fact]
    public async Task AnEmptyResult_IsSkipped()
    {
        var source = Source(DeltaQuery, Delta.Column("ChangeId"));

        Assert.Same(PolledResult.Empty, await source.ReadAsync(new CursorRange("int:2", null), CancellationToken.None));
    }

    private IPolledSource Source(string query, Delta? delta)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Ports:campaigns:Kind"] = "Sqlite",
            ["Ports:campaigns:ConnectionString"] = ConnectionString,
            ["Ports:campaigns:WatermarkStore"] = "statestore",
            ["Ports:campaigns:InitialWatermark"] = "int:0",
        }).Build();
        var services = new ServiceCollection();
        services.AddSqlSource("campaigns", configuration.GetSection("Ports:campaigns"), sql =>
        {
            sql.Query(SqlFile.Inline(query));
            if (delta is not null)
                sql.Delta(delta);
        });
        return services.BuildServiceProvider().GetRequiredKeyedService<IPolledSource>("campaigns");
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100", Justification = "Test setup statements.")]
    private void Execute(string sql)
    {
        using var connection = new SqliteConnection(ConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static int[] Ids(SqlRead read) =>
        JsonDocument.Parse(read.Content).RootElement.EnumerateArray().Select(row => row.GetProperty("ChangeId").GetInt32()).ToArray();
}
