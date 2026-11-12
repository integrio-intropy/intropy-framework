using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.StatefulSweeps;
using Intropy.Framework.Testing.Services;
using Intropy.Framework.Testing.StatefulSweeps;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Adapters.Sql.Test;

/// <summary>
/// A SQL source port through a real extractor, end to end against SQL Server and with the public API
/// only: each run publishes one event with the rows after the committed watermark and commits the
/// watermark; a run whose publish fails keeps its range pending, and the next run publishes exactly
/// that range again (not the rows that arrived since) with the same idempotency key; a full export
/// publishes every row on every run and keeps no watermark.
/// </summary>
[Trait("Category", "Integration")]
[Collection(nameof(SqlServerCollection))]
public sealed class SqlExtractorTests(SqlServerFixture sql) : IAsyncLifetime
{
    private const string Port = "orders";
    private const string Store = "statestore";
    private const string CursorKey = "sql-extractor:orders";

    private readonly FakeIdempotencyServiceClient _idempotency = new();
    private readonly RecordingSender _sender = new();
    private readonly InMemoryCursorStore _store = new();

    public Task InitializeAsync() => sql.ExecuteAsync("""
        IF OBJECT_ID('dbo.ExportOrder') IS NOT NULL DROP TABLE dbo.ExportOrder;
        CREATE TABLE dbo.ExportOrder (Id INT NOT NULL PRIMARY KEY, ModifiedAt DATETIME2 NOT NULL);
        INSERT INTO dbo.ExportOrder VALUES (1, '2026-11-20T01:00:00'), (2, '2026-11-20T02:00:00');
        """);

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task EachRun_PublishesTheRowsAfterTheWatermark_AndCommitsIt()
    {
        await using var provider = Extractor(delta: true);

        var first = await RunAsync(provider);
        await sql.ExecuteAsync("INSERT INTO dbo.ExportOrder VALUES (3, '2026-11-20T03:00:00')");
        var second = await RunAsync(provider);
        var third = await RunAsync(provider);

        Assert.Equal(new RunSummary(1, 0, 0), first);
        Assert.Equal(new RunSummary(1, 0, 0), second);
        Assert.Equal(RunSummary.Empty, third);
        Assert.Equal([[1, 2], [3]], _sender.Sent.Select(Ids));
        Assert.Equal(new CursorState("datetime:2026-11-20T03:00:00.0000000", null), _store.State[CursorKey]);
        Assert.Equal(
            ["orders:2026-01-01T00:00:00:datetime:2026-11-20T02:00:00.0000000",
             "orders:datetime:2026-11-20T02:00:00.0000000:datetime:2026-11-20T03:00:00.0000000"],
            _idempotency.Committed.Select(m => m.Id));
    }

    [Fact]
    public async Task AFailedPublish_KeepsTheRangePending_AndTheNextRunSendsExactlyThatRangeWithTheSameKey()
    {
        await using var provider = Extractor(delta: true);

        _sender.Fail = true;
        var failed = await RunAsync(provider);
        Assert.Equal(new RunSummary(0, 1, 0), failed);
        Assert.Equal(new CursorState("2026-01-01T00:00:00", "datetime:2026-11-20T02:00:00.0000000"), _store.State[CursorKey]);

        // A row arrives before the retry: the pinned re-read must not include it.
        await sql.ExecuteAsync("INSERT INTO dbo.ExportOrder VALUES (3, '2026-11-20T03:00:00')");
        _sender.Fail = false;
        var retried = await RunAsync(provider);
        var next = await RunAsync(provider);

        Assert.Equal(new RunSummary(1, 0, 0), retried);
        Assert.Equal(new RunSummary(1, 0, 0), next);
        Assert.Equal([[1, 2], [3]], _sender.Sent.Select(Ids));
        var checks = _idempotency.StatusChecks.Select(m => m.Id).ToList();
        Assert.Equal(checks[0], checks[1]);
    }

    [Fact]
    public async Task AFullExport_PublishesEveryRowOnEveryRun_AndKeepsNoWatermark()
    {
        await using var provider = Extractor(delta: false);

        await RunAsync(provider);
        await RunAsync(provider);

        Assert.Equal([[1, 2], [1, 2]], _sender.Sent.Select(Ids));
        Assert.Empty(_store.State);
    }

    private ServiceProvider Extractor(bool delta)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "sql-extractor";
            options.ServiceNamespace = "example";
        });
        services.AddSingleton<IIdempotencyServiceClient>(_idempotency);
        services.AddSingleton<IBusinessIncidentServiceClient>(new FakeBusinessIncidentServiceClient());
        services.AddSingleton<SendStep<Context>>(_sender);
        services.AddKeyedSingleton<ICursorStore>(Store, _store);

        services.AddSqlSource(Port, new SqlSourceOptions
        {
            ConnectionString = sql.ConnectionString, WatermarkStore = Store, InitialWatermark = "2026-01-01T00:00:00"
        }, source =>
        {
            if (delta)
                source.Query(SqlFile.Inline("SELECT Id, ModifiedAt FROM dbo.ExportOrder WHERE ModifiedAt > @watermark ORDER BY Id"))
                    .Delta(Delta.Column("ModifiedAt"));
            else
                source.Query(SqlFile.Inline("SELECT Id, ModifiedAt FROM dbo.ExportOrder ORDER BY Id"));
        });
        services.AddExtractor(
            sources => sources.Port(new PolledSourcePort(Port)),
            routes => routes.On<string, string>([Port], (builder, _) =>
            {
                builder
                    .WithDeserializer(new TextDeserializer())
                    .WithValidator(new AcceptValidator())
                    .WithTransformer(new IdentityTransformer())
                    .WithSerializer(new CloudEventSerializeStep<string, Context>(text => Port, _ => DateTimeOffset.UnixEpoch,
                        type: (_, _) => "orders.exported"))
                    .WithSenderFromServices()
                    .WithBusinessIncidents(_ => "unknown", _ => "unknown");
                return delta ? builder.WithSourceIdempotency() : builder;
            }));
        return services.BuildServiceProvider();
    }

    private static Task<RunSummary> RunAsync(IServiceProvider provider) =>
        provider.GetRequiredService<IJob>().ExecuteAsync(CancellationToken.None);

    private static int[] Ids(CloudEvent cloudEvent) =>
        JsonDocument.Parse((string)cloudEvent.Data!).RootElement.EnumerateArray()
            .Select(row => row.GetProperty("Id").GetInt32()).ToArray();

    private sealed class TextDeserializer : DeserializeStep<string, Context>
    {
        public override string StepName => "Deserialize";

        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
            ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>(
                (new BusinessStepResult<string>.Success(Encoding.UTF8.GetString(input.Span)), context));
    }

    private sealed class AcceptValidator : ValidateStep<string, Context>
    {
        public override string StepName => "Validate";

        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(string input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>((new BusinessStepResult<string>.Success(input), context));
    }

    private sealed class IdentityTransformer : TransformStep<string, string, Context>
    {
        public override string StepName => "Transform";

        public override Task<(TechnicalStepResult<string> Result, Context Context)> ExecuteAsync(string input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<string>, Context)>((new TechnicalStepResult<string>.Success(input), context));
    }

    private sealed class RecordingSender : SendStep<Context>
    {
        public List<CloudEvent> Sent { get; } = [];
        public bool Fail { get; set; }

        public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(CloudEvent input,
            Context context, CancellationToken ct)
        {
            if (Fail)
                return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
                    (new TechnicalStepResult<CloudEvent>.Failure(new TechnicalFailure("The broker refused it")), context));
            Sent.Add(input);
            return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
                (new TechnicalStepResult<CloudEvent>.Success(input), context));
        }
    }
}
