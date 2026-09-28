using Intropy.Framework.Adapters.File;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.FileSweeps;

/// <summary>
/// Declares a component's source port: the file adapter behind it, keyed by the port name and
/// configured from <c>Ports:&lt;port&gt;</c>, and which port is the component's source.
/// </summary>
public static class SourcePortServiceCollectionExtensions
{
    /// <summary>
    /// Registers the port the component sweeps: its file adapter, from <c>Ports:&lt;port&gt;</c>, and
    /// the port as the component's <see cref="SourcePort"/>. A component has one source port.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="configuration">The component's configuration.</param>
    /// <param name="completion">What happens to a handled source file. Default: deleted.</param>
    public static IServiceCollection AddSourcePort(this IServiceCollection services, string port,
        IConfiguration configuration, FileCompletion? completion = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        EnsureNoSourcePort(services, port);
        services.AddFileAdapter(port, configuration.GetSection($"{PortServiceCollectionExtensions.PortsSectionName}:{port}"));
        return services.AddSourcePort(port, completion);
    }

    /// <summary>
    /// Declares <paramref name="port"/> the component's <see cref="SourcePort"/> without registering
    /// its adapter: register the <see cref="IFileAdapter"/> keyed by the port name yourself.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="completion">What happens to a handled source file. Default: deleted.</param>
    public static IServiceCollection AddSourcePort(this IServiceCollection services, string port,
        FileCompletion? completion = null)
    {
        EnsureNoSourcePort(services, port);
        services.AddSingleton(new SourcePort(port, completion ?? FileCompletion.Delete));
        return services;
    }

    private static void EnsureNoSourcePort(IServiceCollection services, string port)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        if (services.Any(d => d.ServiceType == typeof(SourcePort)))
            throw new InvalidOperationException(
                $"A source port is already registered; a component sweeps one source (adding '{port}').");
    }
}
