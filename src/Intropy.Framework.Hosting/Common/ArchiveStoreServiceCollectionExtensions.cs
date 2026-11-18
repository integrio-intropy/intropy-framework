using System.Diagnostics.CodeAnalysis;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Common;

/// <summary>
/// Registers the component's archive store: where the payloads it received are copied, so the
/// payload behind a business incident — or, in <see cref="ArchiveMode.OnReceipt"/>, behind any trace —
/// can be looked at after the item itself is gone.
/// </summary>
/// <remarks>
/// <para>
/// Like the claim store, the archive store is not a port: no external system touches it. It is shared
/// by every component in an environment, and the framework keys each copy by component and date
/// (<c>{component}/{yyyy}/{MM}/{dd}/{itemId}_{traceId}</c>). The framework never deletes a copy:
/// expiring them is a storage lifecycle policy, owned by the platform.
/// </para>
/// <para>
/// Registering the store is what makes archiving possible; when it happens is the component's
/// <c>Archive:Mode</c>. Without a store nothing is archived, and an incident is raised as before,
/// without a reference to a copy.
/// </para>
/// <para>
/// Extractors, loaders and the send side of a Transactional Integration archive. The receive side of a
/// Transactional Integration does not: its send side receives exactly the bytes it enqueued, and it
/// cannot raise a business incident. A Transactional Integration whose one send pipeline the caller
/// registers builds that pipeline itself, so it archives only if its builder is configured by the
/// framework — declare the send pipeline as a route for that.
/// </para>
/// </remarks>
public static class ArchiveStoreServiceCollectionExtensions
{
    /// <summary>The configuration section holding the archive store's binding settings.</summary>
    public const string ArchiveStoreSectionName = "ArchiveStore";

    /// <summary>The configuration section holding when, and how visibly, the component archives.</summary>
    public const string ArchiveSectionName = "Archive";

    /// <summary>
    /// The key the archive store's <see cref="IFileAdapter"/> is registered under. Not a port name:
    /// the colon is why it cannot collide with a port the topology mints.
    /// </summary>
    public const string ArchiveStoreKey = "intropy:archive-store";

    /// <summary>
    /// Registers the archive store from the <c>ArchiveStore</c> configuration section, which carries
    /// the same settings as a port (<c>Kind</c>, <c>DaprBindingName</c>, <c>BasePath</c>), and the
    /// component's <see cref="ArchiveOptions"/> from the <c>Archive</c> section.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The component's configuration.</param>
    /// <exception cref="InvalidOperationException">A setting of either section has the wrong
    /// shape.</exception>
    public static IServiceCollection AddArchiveStore(this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddSingleton(new ArchiveOptions().Bind(configuration.GetSection(ArchiveSectionName)));
        return services.AddFileAdapter(ArchiveStoreKey, configuration.GetSection(ArchiveStoreSectionName));
    }

    /// <summary>The archive policy for a pipeline composed in <paramref name="scope"/>, or
    /// <see langword="null"/> when no archive store is registered or the mode is
    /// <see cref="ArchiveMode.Never"/>.</summary>
    internal static ArchivePolicy? PolicyFor(IServiceProvider scope)
    {
        var store = scope.GetKeyedService<IFileAdapter>(ArchiveStoreKey);
        if (store is null)
            return null;

        var options = scope.GetService<ArchiveOptions>() ?? new ArchiveOptions();
        if (options.Mode == ArchiveMode.Never)
        {
            // Worth one line, because the alternative is an operator wondering why the store they
            // registered stays empty.
            scope.GetRequiredService<ILoggerFactory>()
                .CreateLogger(typeof(ArchiveStoreServiceCollectionExtensions))
                .LogInformation("Archive store registered; archive mode {Mode}: archiving nothing", options.Mode);
            return null;
        }

        return new ArchivePolicy(scope.GetRequiredService<FrameworkOptions>().ComponentName, store, options.Mode,
            options.BrowseLink(), scope.GetService<TimeProvider>() ?? TimeProvider.System);
    }
}

/// <summary>When, and how visibly, a component archives the payloads it receives.</summary>
public sealed class ArchiveOptions
{
    /// <summary>When a payload is archived. Default: <see cref="ArchiveMode.OnIncident"/>.</summary>
    public ArchiveMode Mode { get; set; } = ArchiveMode.OnIncident;

    /// <summary>
    /// Where an archived copy can be opened, its key appended — the archive container's address, e.g.
    /// <c>https://&lt;account&gt;.blob.core.windows.net/archive/</c>. Optional: with it, traces and
    /// incidents link straight to the copy; without it, they name its key. Only opens when the storage
    /// can be read anonymously; for storage behind sign-in, use <see cref="BrowseUrlTemplate"/>.
    /// Not together with <see cref="BrowseUrlTemplate"/>.
    /// </summary>
    public Uri? BrowseBaseUrl { get; set; }

    /// <summary>
    /// Where an archived copy can be opened, as a link with the key placed anywhere in it:
    /// <c>{key}</c> is the key as is, <c>{key:encoded}</c> the key URL-encoded (slashes as <c>%2F</c>).
    /// Suits a portal that signs the reader in, such as an Azure portal blob link:
    /// <c>https://portal.azure.com/#view/Microsoft_Azure_Storage/BlobPropertiesBladeV2/storageAccountId/&lt;encoded account resource id&gt;/path/archive%2F{key:encoded}/isDeleted~/false/tabToload~/0</c>.
    /// Optional. Not together with <see cref="BrowseBaseUrl"/>.
    /// </summary>
    [SuppressMessage("Design", "CA1056:URI-like properties should not be strings",
        Justification = "A template with {key} placeholders is not a URI until the key is filled in.")]
    public string? BrowseUrlTemplate { get; set; }

    /// <summary>
    /// Copies the values of an <c>Archive</c> configuration section onto these options: <c>Mode</c>,
    /// <c>BrowseBaseUrl</c> and <c>BrowseUrlTemplate</c>.
    /// </summary>
    /// <param name="configuration">The section holding the values.</param>
    /// <returns>These options, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">A key holds a value of the wrong shape, or both
    /// browse settings are set.</exception>
    public ArchiveOptions Bind(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        if (ComponentConfigurationReader.Enum<ArchiveMode>(configuration, nameof(Mode),
                $"{nameof(ArchiveOptions)}.{nameof(Mode)}") is { } mode)
            Mode = mode;
        if (ComponentConfigurationReader.String(configuration, nameof(BrowseBaseUrl)) is { Length: > 0 } url)
            BrowseBaseUrl = Uri.TryCreate(url, UriKind.Absolute, out var uri)
                ? uri
                : throw new InvalidOperationException(
                    $"{nameof(ArchiveOptions)}.{nameof(BrowseBaseUrl)}: configuration value '{url}' is not an absolute URL.");
        if (ComponentConfigurationReader.String(configuration, nameof(BrowseUrlTemplate)) is { Length: > 0 } template)
            BrowseUrlTemplate = template;

        // At startup rather than on the first archived payload: a wrong link should stop the
        // component, not leave every incident without one.
        BrowseLink();
        return this;
    }

    /// <summary>The browse link these options describe, or <see langword="null"/> when neither browse
    /// setting is set.</summary>
    /// <exception cref="InvalidOperationException">Both are set, or the one set has the wrong shape.</exception>
    internal ArchiveLink? BrowseLink()
    {
        if (BrowseBaseUrl is not null && !string.IsNullOrWhiteSpace(BrowseUrlTemplate))
            throw new InvalidOperationException(
                $"{nameof(ArchiveOptions)}: set {nameof(BrowseBaseUrl)} or {nameof(BrowseUrlTemplate)}, not both.");
        try
        {
            if (!string.IsNullOrWhiteSpace(BrowseUrlTemplate))
                return ArchiveLink.FromTemplate(BrowseUrlTemplate);
            return BrowseBaseUrl is { } baseUrl ? ArchiveLink.FromBaseUrl(baseUrl) : null;
        }
        catch (ArgumentException ex)
        {
            var setting = string.IsNullOrWhiteSpace(BrowseUrlTemplate) ? nameof(BrowseBaseUrl) : nameof(BrowseUrlTemplate);
            throw new InvalidOperationException($"{nameof(ArchiveOptions)}.{setting}: {ex.Message.Split(" (Parameter", 2)[0]}", ex);
        }
    }
}
