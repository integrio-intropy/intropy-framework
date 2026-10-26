namespace Intropy.Framework.Components.Test;

public class ComponentManifestTests
{
    [Fact]
    public void Manifest_WithABlankMember_NamesTheMember()
    {
        var error = Assert.Throws<ArgumentException>(() => new ComponentManifest
        {
            ComponentName = "order-gateway", Organization = "Acme", Publishes = " ",
            EventSource = "urn:Acme:order-gateway",
        });

        Assert.Equal(nameof(ComponentManifest.Publishes), error.ParamName);
        Assert.Contains("Publishes is blank", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Manifest_WithARelativeEventSource_Fails()
    {
        var error = Assert.Throws<ArgumentException>(() => new ComponentManifest
        {
            ComponentName = "order-gateway", Organization = "Acme", Publishes = "order-placed",
            EventSource = "not a uri",
        });

        Assert.Contains("EventSource 'not a uri' is not an absolute URI", error.Message, StringComparison.Ordinal);
    }
}
