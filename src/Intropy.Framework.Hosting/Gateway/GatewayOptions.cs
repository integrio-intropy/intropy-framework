namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// The settings around a gateway's endpoint: how much it accepts, what it tells a caller to wait,
/// and how it waits for its sidecar. Every member has a default; a component sets only what it
/// changes, in code or under the <see cref="SectionName"/> configuration section.
/// </summary>
public sealed class GatewayOptions
{
    /// <summary>The configuration section the options bind from.</summary>
    public const string SectionName = "Gateway";

    /// <summary>The largest request body the gateway reads, in bytes. A larger body is answered
    /// <c>413</c> before any step runs. Default: 1 MiB.</summary>
    public long MaxRequestBodyBytes { get; set; } = 1024 * 1024;

    /// <summary>What a caller is told to wait before it retries a request the gateway could not
    /// take (<c>Retry-After</c> on a <c>503</c>). Default: 5 seconds.</summary>
    public TimeSpan RetryAfter { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Whether the gateway waits for its Dapr sidecar before it reports ready and accepts
    /// requests. Default: true. Only a test host without a sidecar turns it off.</summary>
    public bool WaitForSidecar { get; set; } = true;

    /// <summary>How long one wait for the sidecar lasts before the gateway logs it and waits
    /// again. The gateway never gives up: it stays unready until the sidecar answers. Default:
    /// 60 seconds.</summary>
    public TimeSpan SidecarTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Checks the options; a wrong value fails registration naming the member.</summary>
    /// <exception cref="InvalidOperationException">A member is out of range.</exception>
    internal void Validate()
    {
        if (MaxRequestBodyBytes <= 0)
            throw new InvalidOperationException(
                $"Gateway composition failed: {nameof(MaxRequestBodyBytes)} must be positive, not {MaxRequestBodyBytes}.");
        if (RetryAfter <= TimeSpan.Zero)
            throw new InvalidOperationException(
                $"Gateway composition failed: {nameof(RetryAfter)} must be positive, not {RetryAfter}.");
        if (SidecarTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException(
                $"Gateway composition failed: {nameof(SidecarTimeout)} must be positive, not {SidecarTimeout}.");
    }
}
