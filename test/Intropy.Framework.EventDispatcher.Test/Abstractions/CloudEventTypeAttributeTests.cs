using Intropy.Framework.EventDispatcher.Abstractions;

namespace Intropy.Framework.EventDispatcher.Test.Abstractions;

public class CloudEventTypeAttributeTests
{
    [Fact]
    public void Constructor_WithValidEventType_SetsProperty()
    {
        var attribute = new CloudEventTypeAttribute("order.created");

        Assert.Equal("order.created", attribute.EventType);
    }

    [Fact]
    public void Constructor_WithNullEventType_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new CloudEventTypeAttribute(null!));
    }

    [Fact]
    public void Constructor_WithEmptyEventType_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new CloudEventTypeAttribute(""));
    }

    [Fact]
    public void Constructor_WithWhitespaceEventType_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new CloudEventTypeAttribute("   "));
    }
}
