using System.Diagnostics;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// Records every framework span in the traces a test starts, and in the traces of spans linked to
/// them, leaving every other test's spans to that test's own sampling.
/// </summary>
public sealed class TraceRecorder : IDisposable
{
    private readonly HashSet<ActivityTraceId> _traces;
    private readonly List<Activity> _stopped = [];
    private readonly ActivityListener _listener;

    public TraceRecorder(IEnumerable<ActivityTraceId> traces)
    {
        _traces = [.. traces];
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name.StartsWith("Intropy.Framework.", StringComparison.Ordinal),
            Sample = (ref ActivityCreationOptions<ActivityContext> options) =>
                _traces.Contains(options.TraceId) ||
                (options.Links?.Any(l => _traces.Contains(l.Context.TraceId)) ?? false)
                    ? ActivitySamplingResult.AllDataAndRecorded
                    : ActivitySamplingResult.None,
            ActivityStopped = activity =>
            {
                lock (_stopped) _stopped.Add(activity);
            }
        };
        ActivitySource.AddActivityListener(_listener);
    }

    /// <summary>A traceparent in a trace of its own, recorded by this recorder.</summary>
    public static string NewTraceParent(out ActivityTraceId traceId)
    {
        traceId = ActivityTraceId.CreateRandom();
        return $"00-{traceId}-{ActivitySpanId.CreateRandom()}-01";
    }

    public List<Activity> InTrace(ActivityTraceId traceId)
    {
        lock (_stopped) return [.. _stopped.Where(s => s.TraceId == traceId)];
    }

    /// <summary>The spans with no parent that link to <paramref name="span"/>.</summary>
    public List<Activity> RootsLinkedTo(Activity span)
    {
        lock (_stopped)
            return [.. _stopped.Where(s => s.ParentSpanId == default &&
                                           s.Links.Any(l => l.Context.SpanId == span.SpanId))];
    }

    public void Dispose() => _listener.Dispose();
}
