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
