using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Intropy.Framework.Core.Configuration;

/// <summary>
/// Extension methods for registering the required framework services and configuration.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers Intropy Framework services and configuration with the dependency injection container.
    /// </summary>
    /// <param name="services">The service collection to add services to</param>
    /// <param name="configure">Optional configuration delegate to customize framework options</param>
    /// <returns>The service collection for method chaining</returns>
    public static IServiceCollection AddIntropyFramework(
        this IServiceCollection services,
        Action<FrameworkOptions>? configure = null)
    {
        // Register configuration options
        if (configure != null)
        {
            services.Configure(configure);
        }
        else
        {
            var componentName = Environment.GetEnvironmentVariable(Constants.ComponentNameEnvironmentVariable);
            if (string.IsNullOrEmpty(componentName))
                throw CreateMissingComponentNameException();

            var serviceNamespace = Environment.GetEnvironmentVariable(Constants.ServiceNamespaceEnvironmentVariable);
            if (string.IsNullOrEmpty(serviceNamespace))
                throw CreateMissingServiceNameSpaceException();

            services.Configure<FrameworkOptions>(opts =>
            {
                opts.ComponentName = componentName;
                opts.ServiceNamespace = serviceNamespace;
            });
        }

        // Validate configuration
        services.AddOptions<FrameworkOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();

        // Make options available as singleton for easier access
        services.TryAddSingleton<FrameworkOptions>(serviceProvider =>
            serviceProvider.GetRequiredService<IOptions<FrameworkOptions>>().Value);

        return services;
    }

    private static InvalidOperationException CreateMissingComponentNameException()
    {
        const string message =
            $"Component name is missing. Set it by either add the environment variable {Constants.ComponentNameEnvironmentVariable} " +
            $"or by using the IServiceCollection.{nameof(AddIntropyFramework)}(conf => ...) func";
        return new InvalidOperationException(message);
    }
    
    private static InvalidOperationException CreateMissingServiceNameSpaceException()
    {
        const string message =
            $"ServiceNamespace is missing. Set it by either add the environment variable {Constants.ServiceNamespaceEnvironmentVariable} " +
            $"or by using the IServiceCollection.{nameof(AddIntropyFramework)}(conf => ...) func";
        return new InvalidOperationException(message);
    }
}
