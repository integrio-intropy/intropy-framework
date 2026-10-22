using Intropy.Framework.Components.Gateway;
using Intropy.Framework.Components.Test.Samples;

namespace Intropy.Framework.Components.Test;

public class ComponentManifestTests
{
    [Fact]
    public void From_ReadsTheAttribute()
    {
        var manifest = ComponentManifest.From(typeof(OrderGateway));

        Assert.Equal("order-gateway", manifest.ComponentName);
        Assert.Equal("Acme", manifest.Organization);
        Assert.Equal("order-placed", manifest.Publishes);
        Assert.Equal(typeof(OrderPlaced), manifest.Payload);
        Assert.Equal("order-gateway-endpoint", manifest.Port);
        Assert.Equal("/orders", manifest.EndpointPath);
        Assert.Equal("urn:Acme:order-gateway", manifest.EventSource);
    }

    [Fact]
    public void From_WithoutTheAttribute_NamesTheClass()
    {
        var error = Assert.Throws<InvalidOperationException>(() => ComponentManifest.From(typeof(GatewayComponent)));

        Assert.Contains("GatewayComponent carries no IntropyComponent attribute", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void From_WithAnEmptyMember_NamesTheMember()
    {
        var attribute = OrderGatewayIdentity(publishes: string.Empty, payload: typeof(OrderPlaced));

        var error = Assert.Throws<InvalidOperationException>(
            () => ComponentManifest.From("OrderGateway", attribute));

        Assert.Contains("OrderGateway's IntropyComponent attribute sets no Publishes", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void From_WithoutAPayload_NamesTheMember()
    {
        var attribute = OrderGatewayIdentity(payload: null!);

        var error = Assert.Throws<InvalidOperationException>(
            () => ComponentManifest.From("OrderGateway", attribute));

        Assert.Contains("sets no Payload", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void From_WithARelativeEventSource_Fails()
    {
        var attribute = OrderGatewayIdentity(eventSource: "not a uri", payload: typeof(OrderPlaced));

        var error = Assert.Throws<InvalidOperationException>(
            () => ComponentManifest.From("OrderGateway", attribute));

        Assert.Contains("EventSource 'not a uri', which is not an absolute URI", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void From_WithAnOptionalMemberUnset_LeavesItUnset()
    {
        var attribute = OrderGatewayIdentity(payload: typeof(OrderPlaced), port: null, endpointPath: null);

        var manifest = ComponentManifest.From("OrderGateway", attribute);

        Assert.Null(manifest.Port);
        Assert.Null(manifest.EndpointPath);
    }

    /// <summary>OrderGateway's identity, with the members a check under test disturbed.</summary>
    private static IntropyComponentAttribute OrderGatewayIdentity(
        string publishes = "order-placed", Type? payload = null,
        string eventSource = "urn:Acme:order-gateway", string? port = "order-gateway-endpoint",
        string? endpointPath = "/orders") =>
        new("order-gateway")
        {
            Organization = "Acme",
            Publishes = publishes,
            Payload = payload!,
            EventSource = eventSource,
            Port = port,
            EndpointPath = endpointPath,
        };
}
