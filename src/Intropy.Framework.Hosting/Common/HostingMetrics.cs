using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using Intropy.Framework.Hosting.FileSweeps;

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
        "messaging.client.consumed.messages", "{message}", "Messages delivered to the send pipeline.");

    private static readonly Histogram<double> ProcessDuration = Meter.CreateHistogram(
        "messaging.process.duration", "s", "Duration of processing one message through the send pipeline.",
        advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = MessageDurationBuckets });

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

    /// <summary>Records one message processed through the send pipeline.</summary>
    /// <param name="componentName">The component consuming the message.</param>
    /// <param name="topic">The topic the message was consumed from.</param>
    /// <param name="outcome"><c>processed</c>, <c>skipped</c>, <c>failed</c> or <c>interrupted</c>
    /// (by the host stopping: left for redelivery, but not a failure).</param>
    /// <param name="errorType">Set when <paramref name="outcome"/> is <c>failed</c>.</param>
    /// <param name="duration">How long processing took.</param>
    internal static void RecordProcessedMessage(string componentName, string topic, string outcome, string? errorType,
        TimeSpan duration)
    {
        var tags = new TagList
        {
            { "messaging.system", "dapr" },
            { "messaging.operation.name", "process" },
            { "messaging.destination.name", topic },
            { "intropy.component.name", componentName },
            { "intropy.message.outcome", outcome }
        };
        if (errorType is not null)
            tags.Add("error.type", errorType);
        ConsumedMessages.Add(1, tags);
        ProcessDuration.Record(duration.TotalSeconds, tags);
    }

    internal static string OutcomeName(FileOutcome outcome) => outcome switch
    {
        FileOutcome.Consumed => "consumed",
        FileOutcome.Duplicate => "duplicate",
        FileOutcome.Aborted => "aborted",
        _ => "failed"
    };
}
