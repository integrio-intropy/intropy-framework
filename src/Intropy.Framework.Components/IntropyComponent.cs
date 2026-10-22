using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Components;

/// <summary>
/// The root of every declarative component. A component derives from a block's base class (e.g.
/// <see cref="Gateway.GatewayComponent"/>) and declares only what is its own — its
/// rules and its steps; the framework wires everything else from the component's
/// <see cref="ComponentManifest"/>, and each default is a member the component may override.
/// </summary>
public abstract class IntropyComponent
{
    private protected IntropyComponent()
    {
    }

    /// <summary>Registers the services the component needs beyond what the framework wires — a
    /// client a step resolves, a step registered by hand. Runs before the block registers its
    /// steps, so a step registered here wins over the one found in the component's assembly.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The component's configuration.</param>
    protected virtual void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
    }

    /// <summary>Runs the component under its block's host until it exits; returns the exit code.</summary>
    internal abstract Task<int> RunAsync(string[] args, ComponentManifest manifest);
}
