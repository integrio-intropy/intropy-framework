using System.Diagnostics;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Common;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Job.Helpers;

internal static class DaprActivityHelper
{
    private const string TraceParentKey = "traceparent";
    private const string TraceStateKey = "tracestate";

    internal static Activity? Restore(IReadOnlyDictionary<string, Value> extensions)
    {
        var traceParent = extensions.TryGetValue(TraceParentKey, out var parentValue)
            ? parentValue.StringValue
            : null;

        if (string.IsNullOrEmpty(traceParent))
            return null;

        var traceState = extensions.TryGetValue(TraceStateKey, out var stateValue)
            ? stateValue.StringValue
            : null;

        if (!ActivityContext.TryParse(traceParent, traceState, out var parentContext))
            return null;

        var activity = ActivitySourceProvider.ActivitySource.StartActivity(
            "MessageReceived",
            ActivityKind.Consumer,
            parentContext);

        return activity;
    }
}
