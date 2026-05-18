using System.ComponentModel.DataAnnotations;

namespace Intropy.Framework.Core.Configuration;

/// <summary>
/// Class for configuring common properties that should be used by the library
/// </summary>
public class FrameworkOptions
{
    /// <summary>
    /// The name of the component.
    /// </summary>
    [Required] public required string ComponentName { get; set; }
    
    /// <summary>
    /// The name of the organization, e.g. integrio
    /// </summary>
    [Required] public required string ServiceNamespace { get; set; }
}
