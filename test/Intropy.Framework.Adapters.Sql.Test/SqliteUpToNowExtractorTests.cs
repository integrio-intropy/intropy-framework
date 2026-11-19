using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.StatefulSweeps;
using Intropy.Framework.Testing.Services;
using Intropy.Framework.Testing.StatefulSweeps;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace Intropy.Framework.Adapters.Sql.Test;

/// <summary>
/// A delta read up to now through a real extractor and its stateful sweep, against SQLite: each run
/// publishes what has settled and commits the bound it fixed before reading, also when it found nothing;
/// a run whose publish fails keeps that bound pending, and the next run reads exactly that range again,
/// with a row that committed into it late and without the rows written since.
/// </summary>
public sealed class SqliteUpToNowExtractorTests : IDisposable
{
    private const string Port = "campaigns";
    private const string Store = "statestore";
    private const string CursorKey = "sql-extractor:campaigns";
    private const string Start = "datetimeoffset:2026-01-01T00:00:00+00:00";

    private readonly string _path = Path.Combine(Path.GetTempPath(), $"intropy-sqlite-{Guid.NewGuid():N}.db");
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 7, 1, 0, 0, 0, TimeSpan.Zero));
    private readonly FakeIdempotencyServiceClient _idempotency = new();
    private readonly RecordingSender _sender = new();
    private readonly InMemoryCursorStore _store = new();

    public SqliteUpToNowExtractorTests() => Execute("""
        CREATE TABLE CampaignChange (Id INTEGER PRIMARY KEY, ChangedAt TEXT NOT NULL);
        INSERT INTO CampaignChange VALUES (1, '2026-03-01T08:00:00+00:00'), (2, '2026-06-30T23:58:30+00:00');
        """);

    private string ConnectionString => $"Data Source={_path};Pooling=False";

    public void Dispose() => System.IO.File.Delete(_path);

    [Fact]
    public async Task EachRun_PublishesWhatHasSettled_AndARunWithoutRowsStillMovesTheWatermark()
    {
        await using var provider = Extractor();

        var first = await RunAsync(provider);
        _time.Advance(TimeSpan.FromMinutes(10));
        var second = await RunAsync(provider);

        Assert.Equal(new RunSummary(1, 0, 0), first);
        Assert.Equal(0, second.Processed + second.Failed);
        Assert.Equal([[1, 2]], _sender.Sent.Select(Ids));
        Assert.Equal(new CursorState("datetimeoffset:2026-07-01T00:09:00.0000000+00:00", null), _store.State[CursorKey]);
    }

    [Fact]
    public async Task AFailedPublish_KeepsTheBoundPending_AndTheRetryReadsExactlyThatRange()
    {
        await using var provider = Extractor();

        _sender.Fail = true;
        var failed = await RunAsync(provider);
        Assert.Equal(1, failed.Failed);
        Assert.Equal(new CursorState(Start, "datetimeoffset:2026-06-30T23:59:00.0000000+00:00"), _store.State[CursorKey]);

        // A transaction stamped before the bound commits late; a newer row arrives after it.
        Execute("INSERT INTO CampaignChange VALUES (3, '2026-06-30T23:58:45+00:00'), (4, '2026-07-01T00:05:00+00:00')");
        _time.Advance(TimeSpan.FromMinutes(10));
        _sender.Fail = false;
        await RunAsync(provider);
        await RunAsync(provider);

        Assert.Equal([[1, 2, 3], [4]], _sender.Sent.Select(Ids));
        Assert.Equal(new CursorState("datetimeoffset:2026-07-01T00:09:00.0000000+00:00", null), _store.State[CursorKey]);
    }

    private ServiceProvider Extractor()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "sql-extractor";
            options.ServiceNamespace = "example";
        });
        services.AddSingleton<TimeProvider>(_time);
        services.AddSingleton<IIdempotencyServiceClient>(_idempotency);
        services.AddSingleton<IBusinessIncidentServiceClient>(new FakeBusinessIncidentServiceClient());
        services.AddSingleton<SendStep<Context>>(_sender);
        services.AddKeyedSingleton<ICursorStore>(Store, _store);

        services.AddSqlSource(Port, new SqlSourceOptions
        {
            Kind = SqlSourceOptions.SqliteKind, ConnectionString = ConnectionString, CursorStore = Store, InitialWatermark = Start
        }, sql => sql
            .Query(SqlFile.Inline("SELECT Id, ChangedAt FROM CampaignChange WHERE ChangedAt > @watermark ORDER BY Id"))
            .Delta(Delta.Column("ChangedAt").UpToNow(TimeSpan.FromMinutes(1))));
        services.AddExtractor(
            sources => sources.Port(new PolledSourcePort(Port)),
            routes => routes.On<string, string>([Port], (builder, _) => builder
                .WithDeserializer(new TextDeserializer())
                .WithValidator(new AcceptValidator())
                .WithSourceIdempotency()
                .WithTransformer(new IdentityTransformer())
                .WithSerializer(new CloudEventSerializeStep<string, Context>(_ => Port, _ => DateTimeOffset.UnixEpoch,
                    type: (_, _) => "campaigns.changed"))
                .WithSenderFromServices()
                .WithBusinessIncidents(_ => "unknown", _ => "unknown")));
        return services.BuildServiceProvider();
    }

    private static Task<RunSummary> RunAsync(IServiceProvider provider) =>
        provider.GetRequiredService<IJob>().ExecuteAsync(CancellationToken.None);

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100", Justification = "Test setup statements.")]
    private void Execute(string sql)
    {
        using var connection = new SqliteConnection(ConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static int[] Ids(CloudEvent cloudEvent) =>
        JsonDocument.Parse((string)cloudEvent.Data!).RootElement.EnumerateArray()
            .Select(row => row.GetProperty("Id").GetInt32()).ToArray();
}
