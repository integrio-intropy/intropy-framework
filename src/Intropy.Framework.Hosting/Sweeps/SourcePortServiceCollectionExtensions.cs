using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.FileSweeps;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// Declares a component's single source port: a file port (its file adapter keyed by the port
/// name and configured from <c>Ports:&lt;port&gt;</c>), or a source port of any kind.
/// </summary>
public static class SourcePortServiceCollectionExtensions
{
    /// <summary>
    /// Registers the port the component sweeps: its file adapter, from <c>Ports:&lt;port&gt;</c>, and
    /// the port as the component's <see cref="FileSourcePort"/>. This overload declares a component's
    /// single source port; a component with several declares them with its sources
    /// (<see cref="Sources"/>, by the routes overloads of AddExtractor and
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
    /// Declares <paramref name="port"/> the component's <see cref="FileSourcePort"/> without registering
    /// its adapter: register the <see cref="IFileAdapter"/> keyed by the port name yourself. This
    /// overload declares a component's single source port; a component with several declares them
    /// with its sources (<see cref="Sources"/>, by the routes overloads of AddExtractor and
    /// AddTransactionalIntegration).
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="completion">What happens to a handled source file. Default: deleted.</param>
    public static IServiceCollection AddSourcePort(this IServiceCollection services, string port,
        FileCompletion? completion = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        return services.AddSourcePort(new FileSourcePort(port, completion ?? FileCompletion.Delete));
    }

    /// <summary>
    /// Declares <paramref name="source"/> the component's single source port, whatever kind of
    /// source it is; its adapter is registered separately. A component with several source ports
    /// declares them with its sources (<see cref="Sources"/>, by the routes overloads of
    /// AddExtractor and AddTransactionalIntegration).
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="source">The source port.</param>
    /// <exception cref="InvalidOperationException">A source port is already registered.</exception>
    public static IServiceCollection AddSourcePort(this IServiceCollection services, SourcePort source)
    {
        ArgumentNullException.ThrowIfNull(source);
        EnsureNoSourcePort(services, source.Name);
        services.AddSingleton(source);
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
