using Intropy.Framework.Hosting.FileSweeps;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// The source ports a Transactional Integration sweeps, each with what happens to a file once it is
/// on the integration's queue. Which send pipeline a port's messages go through is declared
/// separately, with <see cref="TransactionalIntegrationRoutes{TCtx}"/>.
/// </summary>
/// <remarks>
/// The ports' file adapters are registered keyed by port name, as for a single source port. The
/// ports are swept one after another, in declaration order.
/// </remarks>
public sealed class TransactionalIntegrationSources
{
    private readonly List<SourcePort> _ports = [];

    /// <summary>Declares a source port to sweep.</summary>
    /// <param name="port">The port name, as declared in the system's topology; the key of the
    /// port's file adapter.</param>
    /// <param name="completion">What happens to a source file once it is on the queue. Default:
    /// deleted.</param>
    /// <returns>The sources, for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="port"/> is empty, or already
    /// declared.</exception>
    public TransactionalIntegrationSources Port(string port, FileCompletion? completion = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        if (_ports.Any(p => string.Equals(p.Name, port, StringComparison.Ordinal)))
            throw new ArgumentException($"Source port '{port}' is already declared.", nameof(port));

        _ports.Add(new SourcePort(port, completion ?? FileCompletion.Delete));
        return this;
    }

    /// <summary>The declared ports, in declaration order.</summary>
    internal IReadOnlyList<SourcePort> Ports => _ports;
}
