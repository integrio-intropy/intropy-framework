using Intropy.Framework.Hosting.StatefulSweeps;
using Intropy.Framework.Hosting.Sweeps;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Adapters.Sql.Test;

/// <summary>
/// A SQL port's configuration is checked when the port is registered, without echoing a value (the
/// connection string is a secret); a delta needs a cursor store and an initial watermark, a full
/// export needs neither; and the query is read from the component's embedded <c>.sql</c> file.
/// </summary>
public class SqlSourceRegistrationTests
{
    private const string Secret = "Server=db;User Id=reader;Password=s3cr3t";

    [Fact]
    public void AFullExport_RegistersAPolledSourceWithoutACursor()
    {
        var services = new ServiceCollection();
        services.AddSqlSource("campaigns", Section(new() { ["Kind"] = "Sql", ["ConnectionString"] = Secret }),
            sql => sql.Query(SqlFile.Inline("SELECT 1 AS One")));

        var source = services.BuildServiceProvider().GetRequiredKeyedService<IPolledSource>("campaigns");

        Assert.Null(source.Cursor);
    }

    [Fact]
    public void AFullExport_AcceptsTheCursorStoreTheHostSetsForEveryReader()
    {
        // The system's host sets Ports:<port>:CursorStore for every component that reads an external
        // port, whether or not the source keeps a cursor.
        var services = new ServiceCollection();
        services.AddSqlSource("campaigns", Section(new()
        {
            ["Kind"] = "Sql", ["ConnectionString"] = Secret, [CursorOptions.StoreKey] = "campaign-extractor-cursors"
        }), sql => sql.Query(SqlFile.Inline("SELECT 1 AS One")));

        var source = services.BuildServiceProvider().GetRequiredKeyedService<IPolledSource>("campaigns");

        Assert.Null(source.Cursor);
    }

    [Fact]
    public void ADelta_KeepsItsCursorInTheConfiguredStore_FromTheInitialWatermark()
    {
        var services = new ServiceCollection();
        services.AddSqlSource("campaigns", Section(new()
        {
            ["Kind"] = "Sql", ["ConnectionString"] = Secret, ["CursorStore"] = "statestore",
            ["InitialWatermark"] = "2026-11-20T01:50:00", ["EmptyResult"] = "Send", ["CommandTimeout"] = "00:10:00"
        }), sql => sql.Query(SqlFile.Embedded("Sql/Query.sql")).Delta(Delta.Column("ModifiedAt")));

        var source = services.BuildServiceProvider().GetRequiredKeyedService<IPolledSource>("campaigns");

        Assert.Equal(new CursorOptions("statestore", "2026-11-20T01:50:00"), source.Cursor);
    }

    [Fact]
    public void TheBuilder_IsReadOnceAtRegistration_SoChangingItLaterChangesNothing()
    {
        var services = new ServiceCollection();
        SqlSourceBuilder? kept = null;
        services.AddSqlSource("campaigns", Section(new() { ["Kind"] = "Sql", ["ConnectionString"] = Secret }), sql =>
        {
            kept = sql;
            sql.Query(SqlFile.Inline("SELECT 1 AS One"));
        });

        // A delta added afterwards was never validated (there is no cursor store) and must not run.
        kept!.Delta(Delta.Column("One"));
        var source = services.BuildServiceProvider().GetRequiredKeyedService<IPolledSource>("campaigns");

        Assert.Null(source.Cursor);
    }

    [Theory]
    [InlineData("int:abc")]
    [InlineData("datetime:not-a-date")]
    [InlineData("binary:0xZZ")]
    public void AnInitialWatermarkThatDoesNotMatchItsTag_IsRejected(string initial)
    {
        var error = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddSqlSource("campaigns",
            Section(new()
            {
                ["Kind"] = "Sql", ["ConnectionString"] = Secret, ["CursorStore"] = "statestore",
                ["InitialWatermark"] = initial
            }), sql => sql.Query(SqlFile.Inline("SELECT 1 AS One")).Delta(Delta.Column("One"))));

        Assert.Contains("InitialWatermark", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(initial, error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("CursorStore")]
    [InlineData("InitialWatermark")]
    public void ADelta_WithoutItsCursorSettings_IsRejected(string missing)
    {
        var settings = new Dictionary<string, string?>
        {
            ["Kind"] = "Sql", ["ConnectionString"] = Secret, ["CursorStore"] = "statestore",
            ["InitialWatermark"] = "2026-11-20T01:50:00"
        };
        settings.Remove(missing);

        var error = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddSqlSource("campaigns",
            Section(settings), sql => sql.Query(SqlFile.Inline("SELECT 1 AS One")).Delta(Delta.Column("One"))));

        Assert.Contains(missing, error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Kind", "Sftp")]
    [InlineData("EmptyResult", "Maybe")]
    [InlineData("EmptyResult", "1")]
    [InlineData("CommandTimeout", "soon")]
    [InlineData("Query", "SELECT 1")]
    public void AnInvalidSetting_IsRejected_WithoutEchoingTheConnectionString(string key, string value)
    {
        var error = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddSqlSource("campaigns",
            Section(new() { ["Kind"] = "Sql", ["ConnectionString"] = Secret, [key] = value }),
            sql => sql.Query(SqlFile.Inline("SELECT 1 AS One"))));

        Assert.DoesNotContain("s3cr3t", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AMissingConnectionString_IsRejected() =>
        Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddSqlSource("campaigns",
            Section(new() { ["Kind"] = "Sql" }), sql => sql.Query(SqlFile.Inline("SELECT 1 AS One"))));

    [Fact]
    public void AMissingQuery_IsRejected() =>
        Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddSqlSource("campaigns",
            Section(new() { ["Kind"] = "Sql", ["ConnectionString"] = Secret }), _ => { }));

    [Fact]
    public void AddSqlSourcePort_RegistersTheSource_AndDeclaresThePort()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Ports:campaigns:Kind"] = "Sql", ["Ports:campaigns:ConnectionString"] = Secret
        }).Build();
        var services = new ServiceCollection();

        services.AddSqlSourcePort("campaigns", configuration, sql => sql.Query(SqlFile.Inline("SELECT 1 AS One")));

        var provider = services.BuildServiceProvider();
        var port = Assert.IsType<PolledSourcePort>(Assert.Single(provider.GetServices<SourcePort>()));
        Assert.Equal("campaigns", port.Name);
        Assert.NotNull(provider.GetKeyedService<IPolledSource>("campaigns"));
    }

    [Fact]
    public void Embedded_ReadsTheFileFromTheCallingAssembly() =>
        Assert.Contains("FROM dbo.Campaign", SqlFile.Embedded("Sql/Query.sql").Text, StringComparison.Ordinal);

    [Fact]
    public void Embedded_WithoutTheResource_SaysHowToEmbedIt()
    {
        var error = Assert.Throws<InvalidOperationException>(() => SqlFile.Embedded("Sql/Missing.sql"));

        Assert.Contains("EmbeddedResource", error.Message, StringComparison.Ordinal);
    }

    private static IConfigurationSection Section(Dictionary<string, string?> settings) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(settings.ToDictionary(kv => $"Ports:campaigns:{kv.Key}", kv => kv.Value))
            .Build()
            .GetSection("Ports:campaigns");
}
