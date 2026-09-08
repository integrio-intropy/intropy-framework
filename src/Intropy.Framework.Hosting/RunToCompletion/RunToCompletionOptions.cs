namespace Intropy.Framework.Hosting.RunToCompletion;

/// <summary>
/// Configuration options for a run-to-completion job host.
/// </summary>
public class RunToCompletionOptions
{
    /// <summary>
    /// The name of the job. Used as the tracing activity name and in log statements,
    /// so multiple jobs are distinguishable in telemetry.
    /// </summary>
    public string JobName { get; set; } = "";

    /// <summary>
    /// The maximum time to wait for the Dapr sidecar to become available, in seconds.
    /// </summary>
    /// <value>Default: 30</value>
    public int SidecarTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// The maximum time to wait for the Dapr sidecar to shut down, in seconds.
    /// Bounded on purpose: a wedged sidecar must not hang the job after the work
    /// has completed — an external scheduler would record a failed run despite a
    /// successful execution.
    /// </summary>
    /// <value>Default: 10</value>
    public int SidecarShutdownTimeoutSeconds { get; set; } = 10;
}
