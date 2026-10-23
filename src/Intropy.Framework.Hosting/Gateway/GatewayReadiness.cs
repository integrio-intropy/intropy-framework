namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// Whether the gateway takes requests: false until its sidecar answers. Read by the endpoint, which
/// answers <c>503</c> until then, and by the readiness probe.
/// </summary>
internal sealed class GatewayReadiness
{
    private volatile bool _ready;

    public bool IsReady => _ready;

    public void MarkReady() => _ready = true;
}
