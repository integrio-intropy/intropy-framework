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

    /// <summary>
    /// The Dapr pub/sub component this component runs on: the system's bus for an extractor or a
    /// loader, the internal-queue pub/sub for a transactional integration. The deployment sets it
    /// through the <c>INTROPY_PUBSUB_NAME</c> environment variable, which wins over a value set in
    /// code: the system topology names its components and picks the one each component uses, so the
    /// framework takes the name as given and derives nothing. Setting it in code is an escape hatch
    /// for tests and runs without a system host.
    /// </summary>
    public string? PubSubName { get; set; }
}
