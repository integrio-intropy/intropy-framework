using System.Text.Json;
using Intropy.Framework.Hosting.StatefulSweeps;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace Intropy.Framework.Adapters.Sql.Test;

/// <summary>
/// A column delta read up to now: the upper bound of a read is fixed before the query runs, as the
/// clock less a settle time, so a row written just before the poll but committed after it is still
/// read by a later run. The bound becomes the watermark once the read is handled, also when it found
/// no rows; a pinned range is read again exactly, whatever the clock says; and the watermark never
/// moves back. SQLite has no clock of its own the adapter asks, so the bound comes from the
/// application's <see cref="TimeProvider"/>, and its text dates are compared as instants.
/// </summary>
public sealed class SqliteUpToNowTests : IDisposable
{
    private const string Query = "SELECT Id, ChangedAt FROM CampaignChange WHERE ChangedAt > @watermark ORDER BY ChangedAt";
    private const string Start = "datetimeoffset:2026-01-01T00:00:00.0000000+00:00";
    private static readonly TimeSpan s_settle = TimeSpan.FromMinutes(1);

    private readonly string _path = Path.Combine(Path.GetTempPath(), $"intropy-sqlite-{Guid.NewGuid():N}.db");
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 7, 1, 0, 0, 0, TimeSpan.Zero));

    public SqliteUpToNowTests() => Execute("""
        CREATE TABLE CampaignChange (Id INTEGER PRIMARY KEY, ChangedAt TEXT NOT NULL);
        INSERT INTO CampaignChange VALUES
            (1, '2026-03-01T08:00:00+00:00'),
            (2, '2026-06-30T23:58:30+00:00'),
            (3, '2026-06-30T23:59:30+00:00');
        """);

    private string ConnectionString => $"Data Source={_path};Pooling=False";

    public void Dispose() => System.IO.File.Delete(_path);

    [Fact]
    public async Task ARead_EndsAtNowLessTheSettleTime_AndThatBoundIsTheNewWatermark()
    {
        var read = await SqlRead.Of(Source().ReadAsync(new CursorRange(Start, null), CancellationToken.None));

        // Row 3 was written 30 s ago, inside the settle time: a later run reads it.
        Assert.Equal([1, 2], Ids(read));
        Assert.Equal("datetimeoffset:2026-06-30T23:59:00.0000000+00:00", read.Cursor);
        Assert.Equal("2026-06-30T23:59:00.0000000+00:00", read.Metadata[SqlContextKeys.WatermarkTo]);
    }

    [Fact]
    public async Task ARunWithoutRows_SendsNothing_AndStillMovesTheWatermark()
    {
        // Nothing after 23:58:30 has settled: row 3 is inside the settle time.
        var page = await SinglePage(Source().ReadAsync(
            new CursorRange("datetimeoffset:2026-06-30T23:58:30.0000000+00:00", null), CancellationToken.None));

        Assert.Empty(page.Items);
        Assert.Equal("datetimeoffset:2026-06-30T23:59:00.0000000+00:00", page.Cursor);
    }

    [Fact]
    public async Task ARunWithoutRows_SendsAnEmptyArray_WhenAskedTo()
    {
        var read = await SqlRead.Of(Source(SqlEmptyResult.Send).ReadAsync(
            new CursorRange("datetimeoffset:2026-06-30T23:58:30.0000000+00:00", null), CancellationToken.None));

        Assert.Empty(Ids(read));
        Assert.Equal("datetimeoffset:2026-06-30T23:59:00.0000000+00:00", read.Cursor);
    }

    [Fact]
    public async Task APinnedRange_IsReadAgainExactly_WhateverTheClockSays()
    {
        var source = Source();
        var first = await SqlRead.Of(source.ReadAsync(new CursorRange(Start, null), CancellationToken.None));

        _time.Advance(TimeSpan.FromHours(1));
        var pinned = await SqlRead.Of(source.ReadAsync(new CursorRange(Start, first.Cursor), CancellationToken.None));

        Assert.Equal([1, 2], Ids(pinned));
    }

    [Fact]
    public async Task TheNextRun_ReadsWhatHasSettledSince()
    {
        var source = Source();
        var first = await SqlRead.Of(source.ReadAsync(new CursorRange(Start, null), CancellationToken.None));

        _time.Advance(TimeSpan.FromMinutes(1));
        var next = await SqlRead.Of(source.ReadAsync(new CursorRange(first.Cursor, null), CancellationToken.None));

        Assert.Equal([3], Ids(next));
        Assert.Equal("datetimeoffset:2026-07-01T00:00:00.0000000+00:00", next.Cursor);
    }

    [Fact]
    public async Task AWatermarkAheadOfTheBound_IsNeverMovedBack()
    {
        var result = await Source().ReadAsync(
            new CursorRange("datetimeoffset:2026-07-01T00:00:00.0000000+00:00", null), CancellationToken.None);

        Assert.Same(PolledResult.Empty, result);
    }

    [Fact]
    public async Task TextDates_AreComparedAsInstants_WhateverTheirOffset()
    {
        // 01:30+02:00 is 23:30 the day before in UTC: before the bound, though it sorts after it as text.
        Execute("INSERT INTO CampaignChange VALUES (4, '2026-07-01T01:30:00+02:00')");

        var read = await SqlRead.Of(Source().ReadAsync(
            new CursorRange("datetimeoffset:2026-06-30T23:00:00.0000000+00:00", null), CancellationToken.None));

        Assert.Equal([2, 4], Ids(read).Order());
    }

    [Fact]
    public void ANegativeSettleTime_IsRejected() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Delta.Column("ChangedAt").UpToNow(TimeSpan.FromSeconds(-1)));

    [Fact]
    public void AnInitialWatermarkThatIsNotADate_IsRejected()
    {
        var error = Assert.Throws<InvalidOperationException>(() => Source(initial: "int:0"));

        Assert.Contains("UpToNow", error.Message, StringComparison.Ordinal);
    }

    private IPolledSource Source(SqlEmptyResult emptyResult = SqlEmptyResult.Skip, string initial = Start)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Ports:campaigns:Kind"] = "Sqlite",
            ["Ports:campaigns:ConnectionString"] = ConnectionString,
            ["Ports:campaigns:CursorStore"] = "statestore",
            ["Ports:campaigns:InitialWatermark"] = initial,
            ["Ports:campaigns:EmptyResult"] = emptyResult.ToString(),
        }).Build();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(_time);
        services.AddSqlSource("campaigns", configuration.GetSection("Ports:campaigns"), sql => sql
            .Query(SqlFile.Inline(Query))
            .Delta(Delta.Column("ChangedAt").UpToNow(s_settle)));
        return services.BuildServiceProvider().GetRequiredKeyedService<IPolledSource>("campaigns");
    }

    private static async Task<PolledPage> SinglePage(Task<PolledResult> read)
    {
        var pages = Assert.IsType<PolledPages>(await read);
        var all = new List<PolledPage>();
        await foreach (var page in pages.Items)
            all.Add(page);
        return Assert.Single(all);
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
        JsonDocument.Parse(read.Content).RootElement.EnumerateArray().Select(row => row.GetProperty("Id").GetInt32()).ToArray();
}
