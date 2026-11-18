namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Turns an archive key into a link a person can open: the <c>dataurl</c> on an incident and the
/// <c>intropy.archive.url</c> tag on a trace.
/// </summary>
/// <remarks>
/// Two shapes. A base URL gets the key appended, which suits storage that can be read anonymously.
/// A template places the key anywhere in the link, which suits a portal that signs the reader in and
/// wants the key URL-encoded in the middle of its address (an Azure portal blob link, for example).
/// </remarks>
internal sealed class ArchiveLink
{
    /// <summary>The template placeholder for the key as is, slashes included.</summary>
    internal const string KeyPlaceholder = "{key}";

    /// <summary>The template placeholder for the key URL-encoded, slashes as <c>%2F</c>.</summary>
    internal const string EncodedKeyPlaceholder = "{key:encoded}";

    // A key shaped like the real ones, to prove a template yields an absolute URL before any payload
    // is archived.
    private const string SampleKey = "component/2026/01/31/item_0123456789abcdef0123456789abcdef";

    private readonly Func<string, string> _build;

    private ArchiveLink(Func<string, string> build) => _build = build;

    /// <summary>A link that appends the key to <paramref name="baseUrl"/>.</summary>
    /// <exception cref="ArgumentException"><paramref name="baseUrl"/> is not absolute.</exception>
    internal static ArchiveLink FromBaseUrl(Uri baseUrl)
    {
        ArgumentNullException.ThrowIfNull(baseUrl);
        if (!baseUrl.IsAbsoluteUri)
            throw new ArgumentException("The browse base URL must be absolute.", nameof(baseUrl));
        var prefix = baseUrl.AbsoluteUri.TrimEnd('/') + "/";
        return new ArchiveLink(key => prefix + key);
    }

    /// <summary>A link that fills <see cref="KeyPlaceholder"/> and <see cref="EncodedKeyPlaceholder"/>
    /// in <paramref name="template"/>.</summary>
    /// <exception cref="ArgumentException"><paramref name="template"/> has no placeholder, or does not
    /// give an absolute URL once filled.</exception>
    internal static ArchiveLink FromTemplate(string template)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(template);
        if (!template.Contains(KeyPlaceholder, StringComparison.Ordinal)
            && !template.Contains(EncodedKeyPlaceholder, StringComparison.Ordinal))
            throw new ArgumentException(
                $"The browse URL template must contain {KeyPlaceholder} or {EncodedKeyPlaceholder}.",
                nameof(template));

        var link = new ArchiveLink(key => Fill(template, key));
        if (!Uri.TryCreate(link.For(SampleKey), UriKind.Absolute, out _))
            throw new ArgumentException("The browse URL template must give an absolute URL.", nameof(template));
        return link;
    }

    /// <summary>The link to <paramref name="key"/>.</summary>
    internal string For(string key) => _build(key);

    private static string Fill(string template, string key) => template
        .Replace(EncodedKeyPlaceholder, Uri.EscapeDataString(key), StringComparison.Ordinal)
        .Replace(KeyPlaceholder, key, StringComparison.Ordinal);
}
