using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// What a source port's sweep is created with (<see cref="SourcePort.CreateSweep"/>): the
/// component's root services, its name and its logging.
/// </summary>
/// <param name="Services">The root provider: the port's adapter and every item scope come from it.</param>
/// <param name="ComponentName">The component name, for logs, errors and telemetry.</param>
/// <param name="LoggerFactory">The logger factory.</param>
public sealed record SourceSweepContext(IServiceProvider Services, string ComponentName, ILoggerFactory LoggerFactory)
{
    /// <summary>The root provider: the port's adapter and every item scope come from it.</summary>
    public IServiceProvider Services { get; } = Services ?? throw new ArgumentNullException(nameof(Services));

    /// <summary>The component name, for logs, errors and telemetry.</summary>
    public string ComponentName { get; } = !string.IsNullOrWhiteSpace(ComponentName)
        ? ComponentName
        : throw new ArgumentException("The component name is required.", nameof(ComponentName));

    /// <summary>The logger factory.</summary>
    public ILoggerFactory LoggerFactory { get; } =
        LoggerFactory ?? throw new ArgumentNullException(nameof(LoggerFactory));
}
