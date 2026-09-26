using System.Diagnostics;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Common;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Helpers;

internal static class DaprActivityHelper
{
    private const string TraceParentKey = "traceparent";
    private const string TraceStateKey = "tracestate";

    /// <summary>
    /// Starts the consumer span for a received message: a child of the trace context propagated
    /// with it, or the root of a new trace when the message carries none (or an unparsable one).
    /// Never a child of whatever span happens to be current on the delivering thread, such as the
    /// job's: a message is its own unit of work.
    /// </summary>
    internal static Activity? Restore(IReadOnlyDictionary<string, Value> extensions)
    {
        var traceParent = extensions.TryGetValue(TraceParentKey, out var parentValue)
            ? parentValue.StringValue
            : null;
        var traceState = extensions.TryGetValue(TraceStateKey, out var stateValue)
            ? stateValue.StringValue
            : null;

        if (string.IsNullOrEmpty(traceParent) ||
            !ActivityContext.TryParse(traceParent, traceState, isRemote: true, out var parentContext))
            parentContext = default;

        // With a default parent, StartActivity falls back to Activity.Current: clear it so a
        // message without trace context starts a new trace. The caller's context is async-local
        // to the message handler, so this does not leak past it.
        Activity.Current = null;

        return ActivitySourceProvider.ActivitySource.StartActivity(
            "MessageReceived",
            ActivityKind.Consumer,
            parentContext);
    }
}
