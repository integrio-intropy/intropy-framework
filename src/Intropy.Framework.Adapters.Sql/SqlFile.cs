using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// The text of a SQL statement, kept with the component's code rather than in configuration: a
/// query's columns have to match the component's DTO, and queries are often long.
/// </summary>
public sealed class SqlFile
{
    private SqlFile(string text, string description)
    {
        Text = text;
        Description = description;
    }

    /// <summary>The statement.</summary>
    public string Text { get; }

    /// <summary>Where the statement came from, for errors.</summary>
    internal string Description { get; }

    /// <summary>
    /// A <c>.sql</c> file embedded in the calling assembly. Embed it in the project file, for example
    /// <c>&lt;EmbeddedResource Include="Sql/*.sql" /&gt;</c>.
    /// </summary>
    /// <param name="path">The file's path in the project, such as <c>Sql/Query.sql</c>.</param>
    /// <returns>The statement.</returns>
    /// <exception cref="InvalidOperationException">No embedded resource, or more than one, matches
    /// <paramref name="path"/>, or the file is empty.</exception>
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static SqlFile Embedded(string path) => Embedded(path, Assembly.GetCallingAssembly());

    /// <summary>A <c>.sql</c> file embedded in <paramref name="assembly"/>.</summary>
    /// <param name="path">The file's path in the project, such as <c>Sql/Query.sql</c>.</param>
    /// <param name="assembly">The assembly the file is embedded in.</param>
    /// <returns>The statement.</returns>
    /// <exception cref="InvalidOperationException">No embedded resource, or more than one, matches
    /// <paramref name="path"/>, or the file is empty.</exception>
    public static SqlFile Embedded(string path, Assembly assembly)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(assembly);

        // A resource is named after the project's root namespace and the file's folders, joined with
        // dots: Sql/Query.sql is embedded as <RootNamespace>.Sql.Query.sql.
        var suffix = path.Trim().Replace('/', '.').Replace('\\', '.').TrimStart('.');
        var matches = assembly.GetManifestResourceNames()
            .Where(name => string.Equals(name, suffix, StringComparison.OrdinalIgnoreCase)
                           || name.EndsWith("." + suffix, StringComparison.OrdinalIgnoreCase))
            .ToList();
        var description = $"'{path}' in {assembly.GetName().Name}";
        switch (matches.Count)
        {
            case 0:
                throw new InvalidOperationException(
                    $"The SQL file {description} is not an embedded resource. Embed it in the project file, for " +
                    "example <EmbeddedResource Include=\"Sql/*.sql\" />.");
            case > 1:
                throw new InvalidOperationException(
                    $"The SQL file {description} matches more than one embedded resource " +
                    $"({string.Join(", ", matches)}); give more of its path.");
        }

        using var stream = assembly.GetManifestResourceStream(matches[0])!;
        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        return Of(reader.ReadToEnd(), description);
    }

    /// <summary>A statement given as text, for a short statement or a test.</summary>
    /// <param name="sql">The statement.</param>
    /// <returns>The statement.</returns>
    /// <exception cref="InvalidOperationException">The statement is empty.</exception>
    public static SqlFile Inline(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);
        return Of(sql, "an inline statement");
    }

    private static SqlFile Of(string text, string description) =>
        string.IsNullOrWhiteSpace(text)
            ? throw new InvalidOperationException($"The SQL file {description} is empty.")
            : new SqlFile(text, description);

    /// <inheritdoc />
    public override string ToString() => Description;
}
