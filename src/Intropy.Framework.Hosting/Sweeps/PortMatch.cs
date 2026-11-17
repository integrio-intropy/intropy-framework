namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// The ports whose items a route of a swept component handles, matched on what the framework
/// records about each item when it sweeps it: the source port it came from. Each port carries one
/// kind of data, so the port decides the route.
/// </summary>
/// <remarks>
/// The port is matched on the value the framework sets from the sweep, never on the context metadata
/// a step can change, so a step cannot misroute an item. The routes' <c>On</c> takes the port names
/// and builds this match: it stays internal, so the selection model can grow without shipping a
/// public builder for one criterion.
/// </remarks>
internal sealed class PortMatch
{
    private readonly List<string> _ports;

    /// <summary>Matches items swept from any of <paramref name="ports"/>, matched exactly.</summary>
    /// <param name="ports">The source port names.</param>
    /// <exception cref="ArgumentException">No port is given, or a port is null or whitespace.</exception>
    internal PortMatch(string[] ports)
    {
        ArgumentNullException.ThrowIfNull(ports);
        if (ports.Length == 0)
            throw new ArgumentException("Name at least one source port.", nameof(ports));
        _ports = [];
        foreach (var port in ports)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(port, nameof(ports));
            if (!_ports.Contains(port, StringComparer.Ordinal))
                _ports.Add(port);
        }
    }

    /// <summary>The ports this match names, deduplicated, in the order they were given.</summary>
    internal IReadOnlyList<string> Ports => _ports;

    /// <summary>Whether an item from <paramref name="port"/> matches. An unknown port never matches.</summary>
    internal bool Matches(string? port) => port is not null && _ports.Contains(port, StringComparer.Ordinal);

    /// <summary>The match as a route name: its ports.</summary>
    internal string Describe() => string.Join("+", _ports);
}
