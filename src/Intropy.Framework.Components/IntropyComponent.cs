using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Components;

/// <summary>
/// The root of every declarative component. A component derives from a block's base class (e.g.
/// <see cref="Gateway.GatewayComponent"/>) and declares only what is its own — its
/// identity, its rules and its steps; the framework wires everything else from the component's
/// <see cref="Manifest"/>, and each default is a member the component may override.
/// </summary>
public abstract class IntropyComponent
{
    private protected IntropyComponent()
    {
    }

    /// <summary>The component's identity: its name, organization, message and event source, and the
    /// endpoint the system declares for it.</summary>
    protected internal abstract ComponentManifest Manifest { get; }

    /// <summary>Registers the services the component needs beyond what the framework wires — the
    /// services its steps take in their constructors. Runs before the block registers the steps the
    /// inputs name, so a step class registered here keeps its own registration.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The component's configuration.</param>
    protected virtual void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
    }

    /// <summary>Runs the component under its block's host until it exits; returns the exit code.</summary>
    internal abstract Task<int> RunAsync(string[] args);
}
