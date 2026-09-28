namespace Intropy.Framework.Core.Common;

/// <summary>
/// The names to subscribe to in the host's OpenTelemetry configuration to collect the framework's
/// telemetry.
/// </summary>
/// <remarks>
/// Intropy.Telemetry already listens to <c>Intropy.*</c>, which covers <see cref="ActivitySources"/>;
/// it subscribes to no meters, so add <see cref="Meters"/> through its <c>ConfigureMetrics</c>.
/// </remarks>
/// <example>
/// With Intropy.Telemetry:
/// <code>
/// builder.Services.AddOpenTelemetry(config =>
/// {
///     config.ServiceName = "order-processor"; // the component name
///     config.ConfigureMetrics = metrics => metrics.AddMeter(IntropyTelemetry.Meters);
/// });
/// </code>
/// With the OpenTelemetry SDK directly:
/// <code>
/// builder.Services.AddOpenTelemetry()
///     .WithTracing(tracing => tracing.AddSource(IntropyTelemetry.ActivitySources))
///     .WithMetrics(metrics => metrics.AddMeter(IntropyTelemetry.Meters));
/// </code>
/// </example>
public static class IntropyTelemetry
{
    /// <summary>
    /// A wildcard matching every framework <see cref="System.Diagnostics.ActivitySource"/>:
    /// <c>Intropy.Framework.Core</c>, <c>.Blocks</c>, <c>.Adapters</c> and <c>.Hosting</c>, and any
    /// package added later.
    /// </summary>
    public const string ActivitySources = "Intropy.Framework.*";

    /// <summary>
    /// A wildcard matching every framework <see cref="System.Diagnostics.Metrics.Meter"/>
    /// (today <c>Intropy.Framework.Hosting</c>), and any package's meter added later.
    /// </summary>
    public const string Meters = "Intropy.Framework.*";
}
