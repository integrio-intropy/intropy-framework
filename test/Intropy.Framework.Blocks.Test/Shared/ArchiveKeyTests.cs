using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Blocks.Test.Shared;

public class ArchiveKeyTests
{
    private static readonly DateTimeOffset Date = new(2026, 10, 28, 23, 30, 0, TimeSpan.FromHours(-2));

    // The date is the UTC date: 23:30 at -02:00 is already the 29th.
    [Fact]
    public void For_PartitionsByTheUtcDate_AndEndsInTheItemAndTraceIds()
    {
        var key = ArchiveKey.For("orders", Date, "order.xml", "4bf92f3577b34da6a3ce929d0e0e4736");

        Assert.Equal("orders/2026/10/29/order.xml_4bf92f3577b34da6a3ce929d0e0e4736", key);
    }

    [Fact]
    public void For_WithoutATrace_EndsInTheItemId()
    {
        Assert.Equal("orders/2026/10/29/msg-1", ArchiveKey.For("orders", Date, "msg-1", null));
    }

    // A file name the key cannot hold is made safe rather than refused: its payload must still be
    // archived, and a separator in it must not open a folder of its own.
    [Theory]
    [InlineData("order 42.xml", "order_42.xml")]
    [InlineData("in/order.xml", "in_order.xml")]
    [InlineData("..", "_")]
    [InlineData("ordre-æøå.xml", "ordre-___.xml")]
    public void For_MakesTheItemIdSafe(string itemId, string expected)
    {
        Assert.Equal($"orders/2026/10/29/{expected}", ArchiveKey.For("orders", Date, itemId, null));
    }

    // The component name is configuration, so it is refused, not rewritten: a rewritten name would
    // file one component's copies under another's.
    [Theory]
    [InlineData("")]
    [InlineData("..")]
    [InlineData("orders/eu")]
    public void For_RefusesAComponentNameThatCannotNameASegment(string componentName)
    {
        Assert.Throws<InvalidOperationException>(() => ArchiveKey.For(componentName, Date, "msg-1", null));
    }
}
