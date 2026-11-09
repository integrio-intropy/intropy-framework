using System.Globalization;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.StatefulSweeps;
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
        nameof(SqlSourceOptions.WatermarkStore), nameof(SqlSourceOptions.InitialWatermark),
        nameof(SqlSourceOptions.CommandTimeout)
    ];

    /// <summary>
    /// Registers the SQL source of <paramref name="port"/>, configured from
    /// <paramref name="section"/> (conventionally <c>Ports:&lt;port&gt;</c>) and keyed by the port name:
    /// the SQL counterpart of a port's file adapter. Declare the port as the component's source the
    /// usual way (<c>AddSourcePort(port)</c>, a definition's <c>SourcePort</c>, or the sources'
    /// <c>Port</c>), or use <see cref="AddSqlSourcePort"/>, which does both.
    /// </summary>
    /// <example>
    /// <code>
    /// services.AddSqlSource("source", configuration.GetSection("Ports:source"), sql => sql
    ///     .Query(SqlFile.Embedded("Sql/Query.sql"))
    ///     .Delta(Delta.Column("MODIFIEDDATETIME")));
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
        var query = builder.QueryFile ?? throw Invalid(location, "No query is set; call Query.");
        Validate(options, builder.DeltaMode is not null, location);
        if (services.Any(d => d.ServiceType == typeof(IPolledSource) && Equals(d.ServiceKey, port)))
            throw Invalid(location, "A polled source is already registered for this port.");

        services.AddKeyedSingleton<IPolledSource>(port, (sp, _) => new SqlSource(port, options, query,
            builder.DeltaMode, builder.DataAvailableFile,
            sp.GetService<ILogger<SqlSource>>() ?? NullLogger<SqlSource>.Instance));
        return services;
    }

    /// <summary>
    /// Registers the SQL source of <paramref name="port"/> from <c>Ports:&lt;port&gt;</c> and declares
    /// the port as the component's source: the SQL counterpart of <c>AddSourcePort(port,
    /// configuration)</c>, for a component with one source port.
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
        return services.AddSourcePort(port);
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
            if (!Settings.Contains(child.Key, StringComparer.OrdinalIgnoreCase) || child.GetChildren().Any())
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
            WatermarkStore = section[nameof(SqlSourceOptions.WatermarkStore)],
            InitialWatermark = section[nameof(SqlSourceOptions.InitialWatermark)],
            CommandTimeout = timeout
        };
    }

    private static void Validate(SqlSourceOptions options, bool hasDelta, string location)
    {
        if (!string.Equals(options.Kind?.Trim(), SqlSourceOptions.SqlKind, StringComparison.OrdinalIgnoreCase))
            throw Invalid(location, "Kind must be Sql.");
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            throw Invalid(location, "ConnectionString is required.");
        if (!Enum.IsDefined(options.EmptyResult))
            throw Invalid(location, "EmptyResult must be Skip or Send.");
        if (options.CommandTimeout <= TimeSpan.Zero)
            throw Invalid(location, "CommandTimeout must be positive.");
        if (!hasDelta)
            return;
        if (string.IsNullOrWhiteSpace(options.WatermarkStore))
            throw Invalid(location, "WatermarkStore is required with a delta: the Dapr state store the watermark is kept in.");
        if (string.IsNullOrWhiteSpace(options.InitialWatermark))
            throw Invalid(location, "InitialWatermark is required with a delta: the watermark before the first run.");
    }

    private static InvalidOperationException Invalid(string location, string reason) =>
        new($"{location}: {reason}");
}
