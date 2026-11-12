namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// A component's source: a port a run-to-completion component (an extractor or a transactional
/// integration) sweeps once per run. The kind of source decides what the port's items are and
/// what happens to an item once the component has handled it: a
/// <see cref="FileSweeps.FileSourcePort"/> lists files and deletes or archives each handled one.
/// </summary>
/// <remarks>
/// <para>
/// Declare a component's source ports with its <see cref="Sources"/> (the routes overloads of
/// AddExtractor and AddTransactionalIntegration), or a single one with <c>AddSourcePort</c>.
/// Routes match on the port name, whatever kind of source the port is.
/// </para>
/// <para>
/// A new kind of source derives from this record and creates its sweep, usually a
/// <see cref="SourceSweep{TEntry, TItem}"/>: the kind supplies only what is its own (listing its
/// entries, reading them, completing them, validating the port), and the base follows every rule a
/// framework sweep follows.
/// </para>
/// </remarks>
public abstract record SourcePort
{
    /// <summary>Creates a source port.</summary>
    /// <param name="name">The port name, as declared in the system's topology.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is empty.</exception>
    protected SourcePort(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
    }

    /// <summary>The port name, as declared in the system's topology.</summary>
    public string Name { get; }

    /// <summary>Creates the sweep of this port for one run. A component calls it once per run;
    /// a custom job can call it to sweep the port with a handler of its own.</summary>
    /// <param name="context">The component's root services, its name and its logging.</param>
    public abstract ISourceSweep CreateSweep(SourceSweepContext context);
}
