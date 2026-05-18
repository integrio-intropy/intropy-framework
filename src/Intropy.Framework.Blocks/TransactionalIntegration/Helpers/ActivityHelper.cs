using System.Diagnostics;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Common;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Helpers;

internal static class ActivityHelper
{
    private const string TraceParentKey = "traceparent";
    private const string TraceStateKey = "tracestate";

    internal static Activity? CreateDetachedActivity(string activityName, Activity? previousActivity)
    {
        var links = previousActivity?.Context is not null
            ? new List<ActivityLink> { new(previousActivity.Context) }
            : null;

        Activity.Current = null;

        return ActivitySourceProvider.ActivitySource.StartActivity(
            name: activityName,
            kind: ActivityKind.Internal,
            links: links
        );
    }

    internal static CloudEvent Propagate(Activity? activity, CloudEvent cloudEvent)
    {
        if (activity == null)
            return cloudEvent;

        if (!string.IsNullOrEmpty(activity.Id))
        {
            var attr = CloudEventAttribute.CreateExtension(TraceParentKey, CloudEventAttributeType.String);
            cloudEvent[attr] = activity.Id;
        }

        if (!string.IsNullOrEmpty(activity.TraceStateString))
        {
            var attr = CloudEventAttribute.CreateExtension(TraceStateKey, CloudEventAttributeType.String);
            cloudEvent[attr] = activity.TraceStateString;
        }

        return cloudEvent;
    }
}
