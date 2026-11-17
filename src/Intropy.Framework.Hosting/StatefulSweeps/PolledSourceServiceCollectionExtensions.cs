using Intropy.Framework.Hosting.Sweeps;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.StatefulSweeps;

/// <summary>
/// Registers polled sources (<see cref="IPolledSource"/>): sources a component reads once per run
/// instead of listing, such as a database query or an HTTP poll.
/// </summary>
public static class PolledSourceServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IPolledSource"/> of <paramref name="port"/>, keyed by the port name:
    /// the polled counterpart of a port's file adapter. Declare the port separately as a
    /// <see cref="PolledSourcePort"/> (the sources' <c>Port</c>, or a definition's <c>Source</c>), or use
    /// <see cref="AddPolledSourcePort"/>, which does both.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="factory">Creates the source from the root provider.</param>
    /// <returns>The service collection.</returns>
    /// <exception cref="InvalidOperationException">A polled source is already registered for the port.</exception>
    public static IServiceCollection AddPolledSource(this IServiceCollection services, string port,
        Func<IServiceProvider, IPolledSource> factory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        ArgumentNullException.ThrowIfNull(factory);
        if (services.Any(d => d.ServiceType == typeof(IPolledSource) && Equals(d.ServiceKey, port)))
            throw new InvalidOperationException($"A polled source is already registered for the source port '{port}'.");

        services.AddKeyedSingleton(port, (sp, _) => factory(sp));
        return services;
    }

    /// <summary>
    /// Registers the <see cref="IPolledSource"/> of <paramref name="port"/> and declares the port as the
    /// component's single source port (<see cref="PolledSourcePort"/>). A component with several source
    /// ports declares them with its sources, and registers each source with
    /// <see cref="AddPolledSource"/>.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="port">The port name, as declared in the system's topology.</param>
    /// <param name="factory">Creates the source from the root provider.</param>
    /// <returns>The service collection.</returns>
    /// <exception cref="InvalidOperationException">A polled source is already registered for the port,
    /// or a source port is already declared.</exception>
    public static IServiceCollection AddPolledSourcePort(this IServiceCollection services, string port,
        Func<IServiceProvider, IPolledSource> factory)
    {
        services.AddPolledSource(port, factory);
        return services.AddSourcePort(new PolledSourcePort(port));
    }
}
