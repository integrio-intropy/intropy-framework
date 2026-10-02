using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.Common;

/// <summary>
/// The Hosting package's metrics: one run-to-completion job run, one swept file, and one consumed
/// message at a time. Subscribe with <c>AddMeter(IntropyTelemetry.Meters)</c>.
/// </summary>
internal static class HostingMetrics
{
    internal const string MeterName = "Intropy.Framework.Hosting";

    private static readonly string? Version = typeof(HostingMetrics).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0];

    private static readonly Meter Meter = new(MeterName, Version);

    // A job runs for seconds to an hour; the default buckets stop at 10 seconds.
    private static readonly double[] JobDurationBuckets = [1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600];

    // The OpenTelemetry messaging conventions' advised boundaries.
    private static readonly double[] MessageDurationBuckets =
        [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10];

    private static readonly Counter<long> JobRuns = Meter.CreateCounter<long>(
        "intropy.job.runs", "{run}", "Run-to-completion job runs, by exit code.");

    private static readonly Histogram<double> JobDuration = Meter.CreateHistogram(
        "intropy.job.duration", "s", "Duration of run-to-completion job runs, sidecar wait and shutdown included.",
        advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = JobDurationBuckets });

    private static readonly Counter<long> SweptFiles = Meter.CreateCounter<long>(
        "intropy.sweep.files", "{file}", "Source files swept, by outcome.");

    private static readonly Counter<long> ConsumedMessages = Meter.CreateCounter<long>(
        "messaging.client.consumed.messages", "{message}",
        "Messages the sidecar delivered to a subscribing block, by outcome.");

    private static readonly Histogram<double> ProcessDuration = Meter.CreateHistogram(
        "messaging.process.duration", "s",
        "Duration of processing one message through a Transactional Integration's send pipeline or a loader route.",
        advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = MessageDurationBuckets });

    private static readonly ConcurrentDictionary<InFlightRegistration, byte> InFlightRegistrations = new();

    static HostingMetrics() =>
        Meter.CreateObservableUpDownCounter("intropy.messaging.active_messages", ObserveActiveMessages, "{message}",
            "Messages a subscribing block is processing.");

    internal static void RecordJobRun(string jobName, int exitCode, TimeSpan duration)
    {
        var tags = new TagList
        {
            { "intropy.job.name", jobName },
            { "process.exit.code", exitCode }
        };
        JobRuns.Add(1, tags);
        JobDuration.Record(duration.TotalSeconds, tags);
    }

    internal static void RecordSweptFile(string componentName, string sourcePort, FileOutcome outcome) =>
        SweptFiles.Add(1, new TagList
        {
            { "intropy.component.name", componentName },
            { "intropy.source.port", sourcePort },
            { "intropy.sweep.outcome", OutcomeName(outcome) }
        });

    /// <summary>Records one message delivered to a subscribing block.</summary>
    /// <param name="componentName">The component consuming the message.</param>
    /// <param name="pubSubName">The pub/sub component the message was delivered from.</param>
    /// <param name="topic">The topic the message was delivered from.</param>
    /// <param name="outcome"><c>processed</c>, <c>skipped</c>, <c>failed</c>, <c>interrupted</c>
    /// (by the host stopping: left for redelivery, but not a failure), <c>unrouted</c> (no loader
    /// route handles its event type) or <c>rejected</c> (delivered after stopping began: left for
    /// redelivery without processing).</param>
    /// <param name="errorType">Set when the message failed, or was unrouted and left for
    /// redelivery.</param>
    /// <param name="duration">How long processing took; <see langword="null"/> when the message
    /// was not processed, so it stays out of <c>messaging.process.duration</c>.</param>
    /// <param name="route">The loader route that handled the message, if any.</param>
    internal static void RecordConsumedMessage(string componentName, string pubSubName, string topic, string outcome,
        string? errorType, TimeSpan? duration, string? route = null)
    {
        var tags = new TagList
        {
            { MessagingAttributes.System, MessagingAttributes.DaprSystem },
            { MessagingAttributes.OperationName, MessagingAttributes.Process },
            { MessagingAttributes.DestinationName, topic },
            { MessagingAttributes.PubSubName, pubSubName },
            { MessagingAttributes.ComponentName, componentName },
            { MessagingAttributes.MessageOutcome, outcome }
        };
        if (errorType is not null)
            tags.Add(MessagingAttributes.ErrorType, errorType);
        if (route is not null)
            tags.Add(MessagingAttributes.LoaderRoute, route);
        ConsumedMessages.Add(1, tags);
        if (duration is { } elapsed)
            ProcessDuration.Record(elapsed.TotalSeconds, tags);
    }

    /// <summary>Reports <paramref name="count"/> as a subscribing block's messages in flight, on
    /// <c>intropy.messaging.active_messages</c>, until the returned registration is disposed.</summary>
    internal static IDisposable TrackActiveMessages(string componentName, string pubSubName, string topic,
        Func<int> count)
    {
        var registration = new InFlightRegistration(new TagList
        {
            { MessagingAttributes.System, MessagingAttributes.DaprSystem },
            { MessagingAttributes.DestinationName, topic },
            { MessagingAttributes.PubSubName, pubSubName },
            { MessagingAttributes.ComponentName, componentName }
        }, count);
        InFlightRegistrations.TryAdd(registration, 0);
        return registration;
    }

    private static IEnumerable<Measurement<int>> ObserveActiveMessages() =>
        InFlightRegistrations.Keys.Select(r => new Measurement<int>(r.Count(), r.Tags));

    internal static string OutcomeName(MessageOutcome outcome) => outcome switch
    {
        MessageOutcome.Processed => "processed",
        MessageOutcome.Skipped => "skipped",
        MessageOutcome.Interrupted => "interrupted",
        MessageOutcome.Unrouted => "unrouted",
        _ => "failed"
    };

    /// <summary>The outcome of a message delivered after stopping began.</summary>
    internal const string RejectedOutcome = "rejected";

    internal static string OutcomeName(FileOutcome outcome) => outcome switch
    {
        FileOutcome.Consumed => "consumed",
        FileOutcome.Duplicate => "duplicate",
        FileOutcome.Aborted => "aborted",
        _ => "failed"
    };

    private sealed class InFlightRegistration(TagList tags, Func<int> count) : IDisposable
    {
        internal TagList Tags => tags;

        internal int Count() => count();

        public void Dispose() => InFlightRegistrations.TryRemove(this, out _);
    }
}
