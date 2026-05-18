using System.Reflection;
using Intropy.Framework.EventDispatcher.Dispatcher;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.EventDispatcher.DependencyInjection;

/// <summary>
/// Extension methods for registering CloudEvent handlers in the dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Scans the specified assemblies for CloudEvent handlers and registers them along with
    /// the <see cref="CloudEventDispatcher"/> and <see cref="HandlerRegistry"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="assemblies">Assemblies to scan for handlers.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddCloudEventHandlers(
        this IServiceCollection services,
        params Assembly[] assemblies)
    {
        return services.AddCloudEventHandlers(configure: null, assemblies);
    }

    /// <summary>
    /// Scans the specified assemblies for CloudEvent handlers and registers them along with
    /// the <see cref="CloudEventDispatcher"/> and <see cref="HandlerRegistry"/>,
    /// with optional dispatcher configuration.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional action to configure <see cref="CloudEventDispatcherOptions"/>.</param>
    /// <param name="assemblies">Assemblies to scan for handlers.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddCloudEventHandlers(
        this IServiceCollection services,
        Action<CloudEventDispatcherOptions>? configure,
        params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);
        
        var registry = new HandlerRegistry();

        foreach (var assembly in assemblies)
        {
            registry.RegisterHandlersFromAssembly(assembly);
        }

        // Register options
        var optionsBuilder = services.AddOptions<CloudEventDispatcherOptions>();
        if (configure is not null)
        {
            optionsBuilder.Configure(configure);
        }

        // Register the registry as singleton
        services.AddSingleton(registry);

        // Register the dispatcher
        services.AddScoped<CloudEventDispatcher>();

        // Register each handler
        foreach (var descriptor in registry.GetAllHandlers())
        {
            services.AddScoped(descriptor.HandlerType);
        }

        return services;
    }
}
