using Intropy.Framework.Hosting.FileSweeps;

namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// The source ports a run-to-completion component sweeps, each with what happens to an item once
/// the component has handled it. Which pipeline an item goes through is declared separately, by
/// the component's routes (<see cref="Extractor.ExtractorRoutes{TCtx}"/>,
/// <see cref="TransactionalIntegration.TransactionalIntegrationRoutes{TCtx}"/>), each naming the
/// ports whose items it handles.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Port(SourcePort)"/> declares a source port of any kind (a <see cref="FileSourcePort"/>,
/// or a kind an adapter package provides), and <see cref="Port(string, FileCompletion?)"/> is the
/// shorthand for a file port. Routes match on the port name, whatever kind of source the port is.
/// </para>
/// <para>
/// Each port's adapter is registered separately (a file port's file adapter keyed by the port
/// name, as for a single source port). The ports are swept one after another, in declaration
/// order.
/// </para>
/// </remarks>
public sealed class Sources
{
    private readonly List<SourcePort> _ports = [];

    /// <summary>Declares a file source port to sweep.</summary>
    /// <param name="port">The port name, as declared in the system's topology; the key of the
    /// port's file adapter.</param>
    /// <param name="completion">What happens to a source file once the component has handled it.
    /// Default: deleted.</param>
    /// <returns>The sources, for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="port"/> is empty, or already
    /// declared.</exception>
    public Sources Port(string port, FileCompletion? completion = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        return Port(new FileSourcePort(port, completion ?? FileCompletion.Delete));
    }

    /// <summary>Declares a source port of any kind to sweep.</summary>
    /// <param name="source">The source port.</param>
    /// <returns>The sources, for chaining.</returns>
    /// <exception cref="ArgumentException">A port named like <paramref name="source"/> is already
    /// declared.</exception>
    public Sources Port(SourcePort source)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (_ports.Any(p => string.Equals(p.Name, source.Name, StringComparison.Ordinal)))
            throw new ArgumentException($"Source port '{source.Name}' is already declared.", nameof(source));

        _ports.Add(source);
        return this;
    }

    /// <summary>The declared ports, in declaration order.</summary>
    internal IReadOnlyList<SourcePort> Ports => _ports;

    /// <summary>Checks that the sources and a component's routes agree: at least one port and one
    /// route are declared, a route for every item is the only route, every port a route names is
    /// declared, and every declared port is handled by exactly one route.</summary>
    /// <param name="matches">The routes' matches, in declaration order; null for a route for every
    /// item.</param>
    /// <param name="componentKind">The component kind, for errors ("Extractor").</param>
    /// <exception cref="InvalidOperationException">The sources and the routes disagree.</exception>
    internal void EnsureRoutedBy(IReadOnlyCollection<PortMatch?> matches, string componentKind)
    {
        if (_ports.Count == 0)
            throw new InvalidOperationException(
                $"{componentKind} composition failed: no source port is declared; declare one with Port.");
        if (matches.Count == 0)
            throw new InvalidOperationException(
                $"{componentKind} composition failed: no route is declared; declare one with On or OnAny.");
        if (matches.Count > 1 && matches.Contains(null))
            throw new InvalidOperationException(
                $"{componentKind} composition failed: a route for every item (OnAny) cannot be combined with other routes.");
        var swept = _ports.Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
        if (matches.SelectMany(m => m?.Ports ?? []).FirstOrDefault(port => !swept.Contains(port)) is { } unknown)
            throw new InvalidOperationException(
                $"{componentKind} composition failed: a route names source port '{unknown}', which is not " +
                "declared; declare it with Port.");
        if (matches.SelectMany(m => m?.Ports ?? []).GroupBy(port => port, StringComparer.Ordinal)
                .FirstOrDefault(g => g.Count() > 1) is { } twice)
            throw new InvalidOperationException(
                $"{componentKind} composition failed: source port '{twice.Key}' is handled by more than one route.");
        if (_ports.FirstOrDefault(port => !matches.Any(m => m is null || m.Matches(port.Name))) is { } unrouted)
            throw new InvalidOperationException(
                $"{componentKind} composition failed: no route handles source port '{unrouted.Name}'.");
    }
}
