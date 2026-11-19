using System.Globalization;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.StatefulSweeps;
using Intropy.Framework.Hosting.Sweeps;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// Registers a SQL Server query as a source port: each run of the component (a job) reads the query
/// into one message, which goes through the component's pipeline like a swept file.
/// </summary>
public static class SqlSourceServiceCollectionExtensions
{
    private static readonly string[] Settings =
    [
        nameof(SqlSourceOptions.Kind), nameof(SqlSourceOptions.ConnectionString), nameof(SqlSourceOptions.EmptyResult),
        nameof(SqlSourceOptions.InitialWatermark),
        nameof(SqlSourceOptions.CommandTimeout)
    ];

    /// <summary>
    /// Registers the SQL source of <paramref name="port"/>, configured from
    /// <paramref name="section"/> (conventionally <c>Ports:&lt;port&gt;</c>) and keyed by the port name:
    /// the SQL counterpart of a port's file adapter. Declare the port as a <see cref="PolledSourcePort"/>
    /// (the sources' <c>Port(new PolledSourcePort(port))</c>, or a definition's <c>Source</c>), or use
    /// <see cref="AddSqlSourcePort"/>, which does both.
    /// </summary>
    /// <example>
    /// <code>
    /// services.AddSqlSource("source", configuration.GetSection("Ports:source"), sql => sql
    ///     .Query(SqlFile.Embedded("Sql/Query.sql"))
    ///     .Delta(Delta.Column("MODIFIEDDATETIME").UpToNow(TimeSpan.FromMinutes(5))));
    /// </code>
    /// </example>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="section">The port's settings (<see cref="SqlSourceOptions"/>).</param>
    /// <param name="configure">The query and, optionally, a delta.</param>
    /// <returns>The service collection.</returns>
    /// <exception cref="InvalidOperationException">The settings or the query are invalid, or a source
    /// is already registered for the port.</exception>
    public static IServiceCollection AddSqlSource(this IServiceCollection services, string port,
        IConfigurationSection section, Action<SqlSourceBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        ArgumentNullException.ThrowIfNull(section);
        return services.AddSqlSource(port, Bind(section, port), configure);
    }

    /// <summary>Registers the SQL source of <paramref name="port"/> with options given in code. See
    /// <see cref="AddSqlSource(IServiceCollection, string, IConfigurationSection, Action{SqlSourceBuilder})"/>.</summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="options">The port's settings.</param>
    /// <param name="configure">The query and, optionally, a delta.</param>
    /// <returns>The service collection.</returns>
    /// <exception cref="InvalidOperationException">The settings or the query are invalid, or a source
    /// is already registered for the port.</exception>
    public static IServiceCollection AddSqlSource(this IServiceCollection services, string port,
        SqlSourceOptions options, Action<SqlSourceBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configure);

        var location = $"SQL source port '{port}'";
        var builder = new SqlSourceBuilder();
        configure(builder);
        // Snapshot the builder now: a caller may keep and change it, and what was validated is what
        // runs. The options are init-only.
        var query = builder.QueryFile ?? throw Invalid(location, "No query is set; call Query.");
        var delta = builder.DeltaMode;
        var dataAvailable = builder.DataAvailableFile;
        Validate(options, delta, location);
        if (services.Any(d => d.ServiceType == typeof(IPolledSource) && Equals(d.ServiceKey, port)))
            throw Invalid(location, "A polled source is already registered for this port.");

        return services.AddPolledSource(port, sp => new SqlSource(port, options, query, delta, dataAvailable,
            sp.GetService<TimeProvider>() ?? TimeProvider.System,
            sp.GetService<ILogger<SqlSource>>() ?? NullLogger<SqlSource>.Instance));
    }

    /// <summary>
    /// Registers the SQL source of <paramref name="port"/> from <c>Ports:&lt;port&gt;</c> and declares
    /// the port as the component's single source port (a <see cref="PolledSourcePort"/>): the SQL
    /// counterpart of <c>AddSourcePort(port, configuration)</c>.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="configuration">The component's configuration.</param>
    /// <param name="configure">The query and, optionally, a delta.</param>
    /// <returns>The service collection.</returns>
    /// <exception cref="InvalidOperationException">The settings or the query are invalid, or a source
    /// port is already declared.</exception>
    public static IServiceCollection AddSqlSourcePort(this IServiceCollection services, string port,
        IConfiguration configuration, Action<SqlSourceBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddSqlSource(port,
            configuration.GetSection($"{PortServiceCollectionExtensions.PortsSectionName}:{port}"), configure);
        return services.AddSourcePort(new PolledSourcePort(port));
    }

    // Never put configuration values or binder exceptions in diagnostics: the connection string is a
    // secret.
    private static SqlSourceOptions Bind(IConfigurationSection section, string port)
    {
        var location = $"{section.Path} (SQL source port '{port}')";
        if (!section.Exists() || section.Value is not null)
            throw Invalid(location, "Not configured. Set Kind to Sql and the ConnectionString.");
        foreach (var child in section.GetChildren())
        {
            // The host's settings are accepted: CursorStore is read below, the others are not the
            // source's to use.
            if ((!Settings.Contains(child.Key, StringComparer.OrdinalIgnoreCase)
                 && !PortServiceCollectionExtensions.HostSettings.Contains(child.Key))
                || child.GetChildren().Any())
                throw Invalid(location, $"Unknown or non-scalar setting '{child.Key}'.");
        }

        var emptyResult = SqlEmptyResult.Skip;
        if (section[nameof(SqlSourceOptions.EmptyResult)] is { } emptyText
            && (!Enum.TryParse(emptyText, true, out emptyResult) || !Enum.IsDefined(emptyResult)
                || int.TryParse(emptyText, CultureInfo.InvariantCulture, out _)))
            throw Invalid(location, "EmptyResult must be Skip or Send.");

        var timeout = TimeSpan.FromMinutes(5);
        if (section[nameof(SqlSourceOptions.CommandTimeout)] is { } timeoutText
            && !TimeSpan.TryParse(timeoutText, CultureInfo.InvariantCulture, out timeout))
            throw Invalid(location, "CommandTimeout must be a time span, such as 00:05:00.");

        return new SqlSourceOptions
        {
            Kind = section[nameof(SqlSourceOptions.Kind)] ?? "",
            ConnectionString = section[nameof(SqlSourceOptions.ConnectionString)],
            EmptyResult = emptyResult,
            CursorStore = section[CursorOptions.StoreKey],
            InitialWatermark = section[nameof(SqlSourceOptions.InitialWatermark)],
            CommandTimeout = timeout
        };
    }

    private static void Validate(SqlSourceOptions options, Delta? delta, string location)
    {
        if (!string.Equals(options.Kind?.Trim(), SqlSourceOptions.SqlKind, StringComparison.OrdinalIgnoreCase)
            && !options.IsSqlite)
            throw Invalid(location, "Kind must be Sql or Sqlite.");
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            throw Invalid(location, "ConnectionString is required.");
        if (!Enum.IsDefined(options.EmptyResult))
            throw Invalid(location, "EmptyResult must be Skip or Send.");
        if (options.CommandTimeout <= TimeSpan.Zero)
            throw Invalid(location, "CommandTimeout must be positive.");
        if (delta is null)
            return;
        if (string.IsNullOrWhiteSpace(options.CursorStore))
            throw Invalid(location, "CursorStore is required with a delta: the Dapr state store the watermark is kept in. " +
                "The system's host sets it for a component that reads a port the topology declares external " +
                "(PortRef.DefineExternal); outside the host, set it yourself.");
        if (string.IsNullOrWhiteSpace(options.InitialWatermark))
            throw Invalid(location, "InitialWatermark is required with a delta: the watermark before the first run.");
        SqlWatermark initial;
        try
        {
            initial = SqlWatermark.Parse(options.InitialWatermark);
        }
        catch (Exception e) when (e is FormatException or OverflowException)
        {
            throw Invalid(location, "InitialWatermark does not match its type tag (datetime:, datetimeoffset:, int:, " +
                "decimal:, binary: or string:).");
        }

        // A read up to now compares the watermark with a time; the value stays out of the message.
        if (delta.Settle is not null && !initial.IsDateTime)
            throw Invalid(location, "InitialWatermark must be a date and time for a delta read up to now (UpToNow), " +
                "such as datetimeoffset:2026-01-01T00:00:00+00:00.");
    }

    private static InvalidOperationException Invalid(string location, string reason) =>
        new($"{location}: {reason}");
}
