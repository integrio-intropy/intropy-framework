using System.Diagnostics;
using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Common;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Core.Configuration;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Helpers;

internal static class CloudEventHelper
{
    internal static CloudEvent Create(SourceItem input, Context context, Activity? activity,
        FrameworkOptions options)
    {
        var cloudEvent = new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            Type = InternalQueueMessageTypes.Received,
            Source = SourceUrn(options),
            DataContentType = "application/octet-stream",
            Data = input.Data
        };

        
        cloudEvent = MessagingTelemetry.Propagate(activity, cloudEvent);
        var attr = CloudEventAttribute.CreateExtension(CloudEventExtensionKeys.Metadata, CloudEventAttributeType.String);
        cloudEvent[attr] = JsonSerializer.Serialize(context.Metadata);

        return cloudEvent;
    }

    /// <summary>A probe of the internal queue: no data, and no metadata to restore.</summary>
    internal static CloudEvent CreateProbe(string probeId, FrameworkOptions options) => new()
    {
        Id = probeId,
        Type = InternalQueueMessageTypes.Probe,
        Source = SourceUrn(options)
    };

    private static Uri SourceUrn(FrameworkOptions options) => new($"urn:${options.ComponentName}");
}
