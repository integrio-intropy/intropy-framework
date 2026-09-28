namespace Intropy.Framework.Hosting.Jobs;

/// <summary>
/// Configuration options for a run-to-completion job host.
/// </summary>
public class JobOptions
{
    /// <summary>
    /// The name of the job. Used as the tracing activity name and in log statements,
    /// so multiple jobs are distinguishable in telemetry. When empty, the component name from
    /// <c>FrameworkOptions</c> (<c>AddIntropyFramework</c>) is used.
    /// </summary>
    public string JobName { get; set; } = "";

    /// <summary>
    /// The maximum time to wait for the Dapr sidecar to become available.
    /// </summary>
    /// <value>Default: 30 seconds</value>
    public TimeSpan SidecarTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The maximum time to wait for the Dapr sidecar to shut down.
    /// Bounded on purpose: a wedged sidecar must not hang the job after the work
    /// has completed — an external scheduler would record a failed run despite a
    /// successful execution.
    /// </summary>
    /// <value>Default: 10 seconds</value>
    public TimeSpan SidecarShutdownTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
