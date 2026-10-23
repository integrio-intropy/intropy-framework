using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.Test.Messaging;

/// <summary>
/// How a loader route turns a delivery's attributes into context metadata: the core attributes
/// under their own keys, and every extension the sidecar delivered under
/// <see cref="CloudEventContextKeys.Extension"/>, whatever JSON kind it arrived as.
/// </summary>
public class LoaderRouteProjectionTests
{
    [Fact]
    public void EveryExtension_IsProjectedUnderItsName()
    {
        var metadata = Project(new Dictionary<string, Value>
        {
            ["tenantid"] = Value.ForString("acme"),
            ["priority"] = Value.ForNumber(5),
            ["ratio"] = Value.ForNumber(0.25),
            ["urgent"] = Value.ForBool(true),
            ["tags"] = Value.ForList(Value.ForString("a"), Value.ForString("b"))
        });

        Assert.Equal("acme", metadata[CloudEventContextKeys.Extension("tenantid")]);
        Assert.Equal("5", metadata[CloudEventContextKeys.Extension("priority")]);
        Assert.Equal("0.25", metadata[CloudEventContextKeys.Extension("ratio")]);
        Assert.Equal("true", metadata[CloudEventContextKeys.Extension("urgent")]);
        Assert.Equal("""[ "a", "b" ]""", metadata[CloudEventContextKeys.Extension("tags")]);
    }

    [Fact]
    public void AnEmptyOrNullExtension_IsLeftOff()
    {
        var metadata = Project(new Dictionary<string, Value>
        {
            ["empty"] = Value.ForString(""),
            ["nothing"] = Value.ForNull()
        });

        Assert.DoesNotContain(CloudEventContextKeys.Extension("empty"), metadata.Keys);
        Assert.DoesNotContain(CloudEventContextKeys.Extension("nothing"), metadata.Keys);
    }

    [Fact]
    public void TimeDeliveredAsAnExtension_IsNormalised()
    {
        // The sidecar delivers time as an extension; the key it shares with Time promises a
        // round-trip timestamp, so the projected value is that, not the raw string.
        var metadata = Project(new Dictionary<string, Value>
        {
            ["time"] = Value.ForString("2026-10-23T12:00:00.000+02:00")
        });

        Assert.Equal("2026-10-23T12:00:00.0000000+02:00", metadata[CloudEventContextKeys.Time]);
    }

    [Fact]
    public void ATimeTheFrameworkCannotRead_IsLeftOff()
    {
        // Not left behind as the raw extension, where idempotency would fail to parse it.
        var metadata = Project(new Dictionary<string, Value> { ["time"] = Value.ForString("yesterday") });

        Assert.DoesNotContain(CloudEventContextKeys.Time, metadata.Keys);
    }

    private static Dictionary<string, string> Project(Dictionary<string, Value> extensions) =>
        LoaderRoute<int, int, Context>.ProjectAttributes(new IncomingMessage("msg-1", "pubsub", "topic",
            "urn:test:source", "com.test.event", "1.0", "application/json", ReadOnlyMemory<byte>.Empty,
            extensions, IsRetry: false));
}
