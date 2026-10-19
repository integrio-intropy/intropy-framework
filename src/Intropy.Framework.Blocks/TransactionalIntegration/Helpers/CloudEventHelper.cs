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
        var cloudEvent = Envelope(input, context, options);

        // A claim-checked payload travels as a pointer: the envelope names where the payload is and
        // carries none of it. The claim check wrote it before this event was built.
        if (input.DataRef is { Length: > 0 } dataRef)
            cloudEvent[Extension(CloudEventExtensionKeys.DataRef)] = dataRef;
        else
            cloudEvent.Data = input.Data;

        return MessagingTelemetry.Propagate(activity, cloudEvent);
    }

    /// <summary>
    /// The item's envelope with every attribute set and no payload: what <see cref="Create"/> fills
    /// in, and what the claim check measures against its threshold.
    /// </summary>
    /// <remarks>
    /// The id is the one the claim check minted when it needed to name the payload's key, so the key
    /// and the message agree; otherwise it is minted here. Measuring this envelope accounts for the
    /// propagated metadata, which can be substantial; it does not account for the trace context, which
    /// <see cref="Create"/> adds afterwards and which is a fixed couple of hundred bytes. Set the
    /// threshold below the broker's limit with room to spare, as a claim check is meant to be.
    /// </remarks>
    internal static CloudEvent Envelope(SourceItem input, Context context, FrameworkOptions options)
    {
        var cloudEvent = new CloudEvent
        {
            Id = input.MessageId is { Length: > 0 } id ? id : Guid.NewGuid().ToString(),
            Type = InternalQueueMessageTypes.Received,
            Source = SourceUrn(options),
            DataContentType = "application/octet-stream"
        };

        cloudEvent[Extension(CloudEventExtensionKeys.Metadata)] = JsonSerializer.Serialize(context.Metadata);
        // The swept file's port, from the sweep itself: the send side routes on it.
        if (input.Port is { Length: > 0 } port)
            cloudEvent[Extension(CloudEventExtensionKeys.SourcePort)] = port;

        return cloudEvent;
    }

    /// <summary>A probe of the internal queue: no data, and no metadata to restore.</summary>
    internal static CloudEvent CreateProbe(string probeId, FrameworkOptions options) => new()
    {
        Id = probeId,
        Type = InternalQueueMessageTypes.Probe,
        Source = SourceUrn(options)
    };

    private static CloudEventAttribute Extension(string name) =>
        CloudEventAttribute.CreateExtension(name, CloudEventAttributeType.String);

    private static Uri SourceUrn(FrameworkOptions options) => new($"urn:{options.ComponentName}");
}
