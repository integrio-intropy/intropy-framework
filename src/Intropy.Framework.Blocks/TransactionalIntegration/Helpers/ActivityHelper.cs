using System.Diagnostics;
using Intropy.Framework.Blocks.Common;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Helpers;

internal static class ActivityHelper
{
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
}
