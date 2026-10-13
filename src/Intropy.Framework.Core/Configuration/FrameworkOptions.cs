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
    /// The name of the system the component is deployed in, e.g. order-flow. The deployment sets it
    /// through the <c>INTROPY_SYSTEM_NAME</c> environment variable, which wins over a value set in
    /// code: setting it in code is an escape hatch for tests and runs without a system topology, and
    /// component code should otherwise leave it unset. Components that name their pub/sub
    /// explicitly never need it.
    /// </summary>
    public string? SystemName { get; set; }

    /// <summary>
    /// The system's pub/sub component, <c>&lt;system&gt;-pubsub</c>, or null when
    /// <see cref="SystemName"/> is unset. Several systems share an environment's namespace and
    /// broker, so each system owns its own Dapr component: its scopes and broker credentials are the
    /// system's alone.
    /// </summary>
    public string? SystemPubSubName => string.IsNullOrEmpty(SystemName) ? null : $"{SystemName}-pubsub";
}
