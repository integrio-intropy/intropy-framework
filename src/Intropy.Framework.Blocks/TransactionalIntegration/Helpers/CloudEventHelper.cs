using System.Diagnostics;
using System.Text.Json;
using CloudNative.CloudEvents;
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
            Type = "transactional-integration.received",
            Source = new Uri($"urn:${options.ComponentName}"),
            DataContentType = "application/octet-stream",
            Data = input.Data
        };

        
        cloudEvent = ActivityHelper.Propagate(activity, cloudEvent);
        var attr = CloudEventAttribute.CreateExtension("metadata", CloudEventAttributeType.String);
        cloudEvent[attr] = JsonSerializer.Serialize(context.Metadata);

        return cloudEvent;
    }
}
