namespace Intropy.Framework.Core.Common;

/// <summary>
/// The names to subscribe to in the host's OpenTelemetry configuration to collect the framework's
/// telemetry.
/// </summary>
/// <example>
/// <code>
/// builder.Services.AddOpenTelemetry()
///     .WithTracing(tracing => tracing.AddSource(IntropyTelemetry.ActivitySources));
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
}
