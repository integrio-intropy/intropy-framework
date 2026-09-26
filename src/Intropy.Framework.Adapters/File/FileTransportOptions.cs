namespace Intropy.Framework.Adapters.File;

/// <summary>Configuration for one keyed transport. Registration takes an isolated snapshot.</summary>
public sealed class FileTransportOptions
{
    /// <summary>The binding kind, required explicitly even for <see cref="FileAdapterKind.Local"/>.</summary>
    public FileAdapterKind? Kind { get; set; }

    /// <summary>The Dapr binding name. Empty (the default) means the binding named after the key, the port name.</summary>
    public string DaprBindingName { get; set; } = "";

    /// <summary>The adapter-specific base path, passed through without normalization.</summary>
    public string BasePath { get; set; } = "";

    /// <summary>Optional file-name pattern. Configured patterns have a one-second match timeout.</summary>
    public string? FileNameRegex { get; set; }
}
