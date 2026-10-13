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
    /// the port as the component's <see cref="SourcePort"/>. This overload declares a component's
    /// single source port; a component with several declares them with its sources
    /// (<see cref="FileSources"/>, by the routes overloads of AddExtractor and
    /// AddTransactionalIntegration).
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
    /// its adapter: register the <see cref="IFileAdapter"/> keyed by the port name yourself. This
    /// overload declares a component's single source port; a component with several declares them
    /// with its sources (<see cref="FileSources"/>, by the routes overloads of AddExtractor and
    /// AddTransactionalIntegration).
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

    /// <summary>Registers the source ports of a component that declares them in one place (the
    /// sources of an extractor or a Transactional Integration with routes): all of them, or none
    /// when a source port is already registered. The ports' adapters are registered separately,
    /// keyed by port name.</summary>
    /// <exception cref="InvalidOperationException">A source port is already registered.</exception>
    internal static void AddSourcePorts(IServiceCollection services, IReadOnlyList<SourcePort> ports)
    {
        EnsureNoSourcePort(services, string.Join("', '", ports.Select(p => p.Name)));
        foreach (var port in ports)
            services.AddSingleton(port);
    }

    private static void EnsureNoSourcePort(IServiceCollection services, string port)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        if (services.Any(d => d.ServiceType == typeof(SourcePort)))
            throw new InvalidOperationException(
                $"A source port is already registered; a component declares its source ports once (adding '{port}'); " +
                "a component with several source ports declares them with its sources and routes (the " +
                "AddExtractor/AddTransactionalIntegration routes overloads).");
    }
}
