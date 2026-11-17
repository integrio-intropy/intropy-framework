using System.Text;

namespace Intropy.Framework.Hosting.Sweeps;

/// <summary>
/// One item a source port's sweep hands to the component, inside the item's own
/// dependency-injection scope: a file, for a file source (<see cref="FileSweeps.SweptFile"/>).
/// The component handles every kind of item the same way; only the source knows what it is.
/// </summary>
public abstract class SweptItem
{
    private static readonly IReadOnlyDictionary<string, string> NoMetadata = new Dictionary<string, string>();

    /// <summary>Creates an item.</summary>
    /// <param name="id">The item's identity in its source: a file's name.</param>
    /// <param name="port">The source port the item was swept from.</param>
    /// <param name="services">The item's own scope.</param>
    /// <exception cref="ArgumentException"><paramref name="id"/> or <paramref name="port"/> is empty.</exception>
    protected SweptItem(string id, string port, IServiceProvider services)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(port);
        ArgumentNullException.ThrowIfNull(services);
        Id = id;
        Port = port;
        Services = services;
    }

    /// <summary>The item's identity in its source, for logs, errors and incidents: a file's name.</summary>
    public string Id { get; }

    /// <summary>The source port the item was swept from.</summary>
    public string Port { get; }

    /// <summary>The item's own scope: resolve per-item services (such as the pipeline) here.</summary>
    public IServiceProvider Services { get; }

    /// <summary>What the source records about the item in the pipeline context's metadata, such as
    /// a file's name. The framework adds the source port and the pickup time itself.</summary>
    public virtual IReadOnlyDictionary<string, string> Metadata => NoMetadata;

    /// <summary>Reads the item's content.</summary>
    /// <param name="ct">A cancellation token forwarded to the source.</param>
    public abstract Task<byte[]> ReadAsync(CancellationToken ct = default);

    /// <summary>Reads the item's content as text, decoded with <paramref name="encoding"/> from
    /// the bytes <see cref="ReadAsync"/> returns; empty content reads as <see cref="string.Empty"/>.</summary>
    /// <param name="encoding">The encoding to use to decode the binary content.</param>
    /// <param name="ct">A cancellation token forwarded to the source.</param>
    public async Task<string> ReadTextAsync(Encoding encoding, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(encoding);
        return encoding.GetString(await ReadAsync(ct));
    }
}
