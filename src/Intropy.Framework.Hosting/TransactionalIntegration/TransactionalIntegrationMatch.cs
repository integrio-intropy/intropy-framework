using System.Text.RegularExpressions;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// Which files a send route of a Transactional Integration handles, by what the framework records
/// about each file when it sweeps it: the source port it came from and its name. Every criterion
/// that is set must match; within a criterion, any of its values may.
/// </summary>
/// <remarks>
/// The criteria are matched on values the framework sets from the sweep, never on the context
/// metadata a step can change, so a step cannot misroute a file.
/// </remarks>
public sealed class TransactionalIntegrationMatch
{
    private readonly List<string> _ports = [];
    private readonly List<(string Pattern, Regex Regex)> _fileNames = [];

    /// <summary>Matches files swept from any of <paramref name="ports"/>, matched exactly.</summary>
    /// <param name="ports">The source port names.</param>
    /// <returns>The match, for chaining.</returns>
    /// <exception cref="ArgumentException">No port is given, or a port is empty.</exception>
    public TransactionalIntegrationMatch Port(params string[] ports)
    {
        ArgumentNullException.ThrowIfNull(ports);
        if (ports.Length == 0)
            throw new ArgumentException("Name at least one source port.", nameof(ports));
        foreach (var port in ports)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(port, nameof(ports));
            if (!_ports.Contains(port, StringComparer.Ordinal))
                _ports.Add(port);
        }

        return this;
    }

    /// <summary>Matches files whose name matches any of <paramref name="patterns"/>: <c>*</c>
    /// matches any run of characters and <c>?</c> one character, ignoring case
    /// (<c>"*.csv"</c>, <c>"orders-??.xml"</c>).</summary>
    /// <param name="patterns">The file name patterns.</param>
    /// <returns>The match, for chaining.</returns>
    /// <exception cref="ArgumentException">No pattern is given, or a pattern is empty.</exception>
    public TransactionalIntegrationMatch FileName(params string[] patterns)
    {
        ArgumentNullException.ThrowIfNull(patterns);
        if (patterns.Length == 0)
            throw new ArgumentException("Name at least one file name pattern.", nameof(patterns));
        foreach (var pattern in patterns)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(pattern, nameof(patterns));
            var regex = new Regex("^" + Regex.Escape(pattern).Replace(@"\*", ".*", StringComparison.Ordinal)
                .Replace(@"\?", ".", StringComparison.Ordinal) + "$",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
            _fileNames.Add((pattern, regex));
        }

        return this;
    }

    /// <summary>The ports this match names, or empty when it matches any port.</summary>
    internal IReadOnlyList<string> Ports => _ports;

    /// <summary>Whether no criterion is set.</summary>
    internal bool IsEmpty => _ports.Count == 0 && _fileNames.Count == 0;

    /// <summary>Whether a file from <paramref name="port"/> named <paramref name="fileName"/> matches.
    /// An unknown value never matches a criterion on it.</summary>
    internal bool Matches(string? port, string? fileName) =>
        (_ports.Count == 0 || (port is not null && _ports.Contains(port, StringComparer.Ordinal))) &&
        (_fileNames.Count == 0 || (fileName is not null && _fileNames.Any(f => f.Regex.IsMatch(fileName))));

    /// <summary>The match as a route name: its ports and file name patterns.</summary>
    internal string Describe()
    {
        var ports = _ports.Count == 0 ? "*" : string.Join("+", _ports);
        return _fileNames.Count == 0 ? ports : $"{ports}({string.Join("|", _fileNames.Select(f => f.Pattern))})";
    }
}
