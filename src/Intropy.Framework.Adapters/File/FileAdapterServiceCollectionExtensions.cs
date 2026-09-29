using System.Text.RegularExpressions;
using Dapr.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Registers validated, lazy keyed singleton file adapters without contacting Dapr. The key is the
/// port name; the adapter talks to the Dapr binding of the same name unless
/// <see cref="FileTransportOptions.DaprBindingName"/> says otherwise.
/// </summary>
public static class FileAdapterServiceCollectionExtensions
{
    /// <summary>
    /// Registers a snapshot of a caller-owned section, conventionally <c>Ports:&lt;port&gt;</c>.
    /// Unknown or nested settings are rejected; the host's <c>RootPath</c> is accepted and ignored.
    /// </summary>
    public static IServiceCollection AddFileAdapter(this IServiceCollection services, string key,
        IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(section);
        var location = $"{section.Path} (key '{key}')";
        if (!section.Exists() || section.Value is not null)
            throw Invalid(location, "Not configured. Set Kind to Local, Sftp or AzureBlob (for example the environment variable " +
                $"{section.Path.Replace(":", "__", StringComparison.Ordinal)}__Kind).");

        // Bind only the documented scalar settings. Never include configuration values or
        // binder exceptions in diagnostics: an accidentally supplied value may be a secret.
        foreach (var child in section.GetChildren())
        {
            if (!IsSetting(child.Key) || child.GetChildren().Any())
                throw Invalid(location, $"Unknown or non-scalar setting '{child.Key}'.");
        }

        // The kind is named, never numbered: a numeric value silently changes meaning when the
        // enum is reordered, and the error message is written for configuration authors.
        var text = section[nameof(FileTransportOptions.Kind)];
        if (string.IsNullOrWhiteSpace(text) ||
            !Enum.TryParse<FileAdapterKind>(text, true, out var kind) || !Enum.IsDefined(kind) ||
            !string.Equals(text.Trim(), kind.ToString(), StringComparison.OrdinalIgnoreCase))
            throw Invalid(location, "Kind must explicitly select Local, Sftp or AzureBlob.");

        return Register(services, key, new FileTransportOptions
        {
            Kind = kind,
            DaprBindingName = section[nameof(FileTransportOptions.DaprBindingName)] ?? key,
            BasePath = section[nameof(FileTransportOptions.BasePath)] ?? "",
            FileNameRegex = section[nameof(FileTransportOptions.FileNameRegex)]
        }, location);
    }

    /// <summary>Registers a snapshot of caller-owned options. Subsequent changes require restart.</summary>
    public static IServiceCollection AddFileAdapter(this IServiceCollection services, string key,
        FileTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(options);
        return Register(services, key, options, $"File adapter key '{key}'");
    }

    private static IServiceCollection Register(IServiceCollection services, string key,
        FileTransportOptions options, string location)
    {
        if (services.Any(d => d.ServiceType == typeof(Registration) && Equals(d.ServiceKey, key)))
            throw Invalid(location, "A framework file adapter is already registered for this key. Use keyed DI removal/replacement to override it.");
        if (options.Kind is not { } kind || !Enum.IsDefined(kind))
            throw Invalid(location, "Kind must explicitly select Local, Sftp or AzureBlob.");
        // The key is the port name, and the binding is named after the port unless set explicitly.
        var bindingName = string.IsNullOrEmpty(options.DaprBindingName) ? key : options.DaprBindingName;
        if (string.IsNullOrWhiteSpace(bindingName))
            throw Invalid(location, "DaprBindingName cannot be blank.");
        if (options.BasePath is null)
            throw Invalid(location, "BasePath cannot be null; use an empty string for the binding root.");

        Regex? regex = null;
        if (options.FileNameRegex is not null)
        {
            try
            {
                regex = new Regex(options.FileNameRegex, RegexOptions.None, TimeSpan.FromSeconds(1));
            }
            catch (ArgumentException)
            {
                throw Invalid(location, "FileNameRegex is invalid.");
            }
        }

        var snapshot = new FileAdapterOptions(bindingName, options.BasePath, regex);
        services.AddKeyedSingleton<Registration>(key, new Registration());
        services.AddKeyedSingleton<IFileAdapter>(key, (provider, _) =>
        {
            var client = provider.GetService<DaprClient>() ?? throw Invalid(location,
                "DaprClient is not registered. Register the application's DaprClient before resolving this adapter.");
            return kind switch
            {
                FileAdapterKind.Local => new LocalFileAdapter(client, snapshot),
                FileAdapterKind.Sftp => new SftpAdapter(client, snapshot),
                FileAdapterKind.AzureBlob => new AzureBlobStorageAdapter(client, snapshot),
                _ => throw Invalid(location, "Unsupported Kind.")
            };
        });
        return services;
    }

    /// <summary>
    /// Registers a custom file adapter: <paramref name="factory"/> runs at first resolution,
    /// inside the provider; the component owns everything else (options, the DaprClient
    /// lifetime). Use this when the binding kind has no <see cref="FileAdapterKind"/> or the
    /// adapter needs otherwise-impossible construction — a custom binding kind, a test fake,
    /// or an adapter assembled from services the provider resolves lazily.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="key">The registered key — conventionally the port name.</param>
    /// <param name="factory">Runs at first resolution, inside the provider. The factory is
    /// invoked once; the adapter is a lazy keyed singleton.</param>
    public static IServiceCollection AddFileAdapter(this IServiceCollection services, string key,
        Func<IServiceProvider, IFileAdapter> factory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(factory);
        return RegisterFactory(services, key, factory, $"File adapter key '{key}'");
    }

    private static IServiceCollection RegisterFactory(IServiceCollection services, string key,
        Func<IServiceProvider, IFileAdapter> factory, string location)
    {
        if (services.Any(d => d.ServiceType == typeof(Registration) && Equals(d.ServiceKey, key)))
            throw Invalid(location, "A framework file adapter is already registered for this key. Use keyed DI removal/replacement to override it.");
        services.AddKeyedSingleton<Registration>(key, new Registration());
        services.AddKeyedSingleton<IFileAdapter>(key, (provider, _) => factory(provider));
        return services;
    }

    // The system host sets Ports:<port>:RootPath for components that read the folder directly. It
    // belongs to the port, not to the Dapr-backed adapter, which reaches the folder through the binding.
    private const string HostRootPathSetting = "RootPath";

    private static bool IsSetting(string key) =>
        key.Equals(HostRootPathSetting, StringComparison.OrdinalIgnoreCase) ||
        key.Equals(nameof(FileTransportOptions.Kind), StringComparison.OrdinalIgnoreCase) ||
        key.Equals(nameof(FileTransportOptions.DaprBindingName), StringComparison.OrdinalIgnoreCase) ||
        key.Equals(nameof(FileTransportOptions.BasePath), StringComparison.OrdinalIgnoreCase) ||
        key.Equals(nameof(FileTransportOptions.FileNameRegex), StringComparison.OrdinalIgnoreCase);

    private static InvalidOperationException Invalid(string location, string message) => new($"{location}: {message}");

    private sealed class Registration;
}
