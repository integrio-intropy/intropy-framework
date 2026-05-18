using System.Text.RegularExpressions;

namespace Intropy.Framework.Adapters.File;

/// <summary>
/// Configuration options for the <see cref="IFileAdapter"/>.
/// </summary>
public class FileAdapterOptions
{
    /// <summary>
    /// The name of the Dapr component to use.
    /// </summary>
    public string DaprBindingName { get; }

    /// <summary>
    /// A base path used when performing list, get, create and delete operations.
    /// </summary>
    public string BasePath { get; }

    /// <summary>
    /// A regex used to match file names when listing.
    /// </summary>
    public Regex? FileNameRegex { get; }

    /// <summary>
    /// Creates an instance of <see cref="FileAdapterOptions"/>.
    /// </summary>
    /// <param name="daprBindingName">The name of the Dapr component to use.</param>
    /// <param name="basePath">A base path used when performing list, get, create and delete operations.</param>
    /// <param name="fileNameRegex">A regex used to match file names when listing.</param>
    public FileAdapterOptions(string daprBindingName, string basePath = "", Regex? fileNameRegex = null)
    {
        DaprBindingName = daprBindingName;
        BasePath = basePath;
        FileNameRegex = fileNameRegex;
    }
}