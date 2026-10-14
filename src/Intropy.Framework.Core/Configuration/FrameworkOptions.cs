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
    /// The Dapr pub/sub component of the system's bus, which extractors and loaders share. The
    /// deployment sets it through the <c>INTROPY_SYSTEM_PUBSUB_NAME</c> environment variable, which
    /// wins over a value set in code: the system host names its components, so the framework takes
    /// the name as given and derives nothing. Setting it in code is an escape hatch for tests and
    /// runs without a system host.
    /// </summary>
    public string? SystemPubSubName { get; set; }

    /// <summary>
    /// The Dapr pub/sub component that carries the transactional integrations' internal queues. It
    /// is apart from <see cref="SystemPubSubName"/> because an internal queue has a single consumer:
    /// on brokers that tell queues from topics (Azure Service Bus) the system host renders it as a
    /// queue component and the bus as a topic component. The deployment sets it through the
    /// <c>INTROPY_INTERNAL_QUEUE_PUBSUB_NAME</c> environment variable, which wins over a value set in
    /// code, as for <see cref="SystemPubSubName"/>.
    /// </summary>
    public string? InternalQueuePubSubName { get; set; }
}
