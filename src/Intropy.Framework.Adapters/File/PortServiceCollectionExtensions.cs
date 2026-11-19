using System.Collections.Frozen;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Registers a component's destination ports: the file adapter behind each one, keyed by the
/// port name and configured from <c>Ports:&lt;port&gt;</c>. The component's source port is
/// declared by <c>Intropy.Framework.Hosting</c> (<c>AddSourcePort</c>).
/// </summary>
public static class PortServiceCollectionExtensions
{
    /// <summary>The configuration section holding each port's settings.</summary>
    public const string PortsSectionName = "Ports";

    /// <summary>The setting the system's host writes into a file port's section: the folder the port
    /// resolves to locally, for components that read it directly.</summary>
    public const string RootPathSetting = "RootPath";

    /// <summary>The setting the system's host writes into the section of an external port a component
    /// reads: the Dapr state store the topology minted for the component's cursors.</summary>
    public const string CursorStoreSetting = "CursorStore";

    /// <summary>
    /// The settings the system's host writes into a port's section (<c>Ports:&lt;port&gt;</c>), whatever
    /// the port's kind. An adapter that rejects unknown settings accepts these, and ignores those it
    /// has no use for, so the host never stops a component from starting. Compared ignoring case, as
    /// configuration keys are.
    /// </summary>
    /// <remarks>Add a setting here when the host starts writing one.</remarks>
    public static IReadOnlySet<string> HostSettings { get; } =
        new[] { RootPathSetting, CursorStoreSetting }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Registers a port the component writes through: its file adapter, keyed by the port name and
    /// configured from <c>Ports:&lt;port&gt;</c>. Resolve it with
    /// <c>GetRequiredKeyedService&lt;IFileAdapter&gt;(port)</c>. A component may have several.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="configuration">The component's configuration.</param>
    public static IServiceCollection AddDestinationPort(this IServiceCollection services, string port,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        ArgumentNullException.ThrowIfNull(configuration);
        return services.AddFileAdapter(port, configuration.GetSection($"{PortsSectionName}:{port}"));
    }
}
