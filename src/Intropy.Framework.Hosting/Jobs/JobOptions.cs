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

    /// <summary>
    /// When the Dapr sidecar is shut down after the job. The deployment sets it with the
    /// <c>INTROPY_JOB_SIDECAR_SHUTDOWN</c> environment variable; a value set in code wins.
    /// </summary>
    /// <value>Default: <see cref="Jobs.SidecarShutdown.Always"/></value>
    public SidecarShutdown SidecarShutdown { get; set; } = SidecarShutdown.Always;

    internal const string SidecarShutdownEnvironmentVariable = "INTROPY_JOB_SIDECAR_SHUTDOWN";

    /// <summary>Parses the deployment's <c>INTROPY_JOB_SIDECAR_SHUTDOWN</c>; unset or empty is
    /// null. Anything but a member name fails, so a typo cannot silently keep the default.</summary>
    internal static SidecarShutdown? ParseSidecarShutdown(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        foreach (var member in Enum.GetValues<SidecarShutdown>())
        {
            if (string.Equals(member.ToString(), value.Trim(), StringComparison.OrdinalIgnoreCase))
                return member;
        }
        throw new InvalidOperationException(
            $"{SidecarShutdownEnvironmentVariable} is '{value}'; expected " +
            $"{string.Join(" or ", Enum.GetNames<SidecarShutdown>())}.");
    }
}
