using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Blocks.Test.Shared;

/// <summary>
/// The claim key format, which the receive side mints and the send side validates a pointer against
/// before handing it to a file adapter. The rejections matter most: an adapter joins a key onto its
/// base path without normalising it, so a pointer that is not a key must never reach one.
/// </summary>
public class ClaimKeyTests
{
    private static readonly DateTimeOffset WrittenAt = new(2026, 3, 7, 9, 15, 0, TimeSpan.Zero);

    [Fact]
    public void For_MintsAKeyNamingTheComponentTheDateAndTheMessage()
    {
        var key = ClaimKey.For("orders-integration", WrittenAt, "0b7d2f2e-9c41-4a0e-8c2a-1f2b3c4d5e6f");

        Assert.Equal("orders-integration/2026/03/07/0b7d2f2e-9c41-4a0e-8c2a-1f2b3c4d5e6f", key);
    }

    [Fact]
    public void For_PadsTheDate_SoEverySegmentIsAFixedWidth()
    {
        // A single-digit month would otherwise mint a key the reader rejects, and the payload would
        // already be stored by then.
        var key = ClaimKey.For("orders", new DateTimeOffset(2026, 3, 7, 0, 0, 0, TimeSpan.Zero), "m1");

        Assert.Equal("orders/2026/03/07/m1", key);
        Assert.True(ClaimKey.IsValid(key));
    }

    [Fact]
    public void IsValid_AcceptsWhatForMints()
    {
        // The one test that keeps the two sides agreeing: whatever For produces must resolve.
        Assert.True(ClaimKey.IsValid(ClaimKey.For("orders-integration", WrittenAt, Guid.NewGuid().ToString())));
    }

    [Fact]
    public void IsValid_AcceptsAnotherComponentsKey()
    {
        // The component segment is checked for shape, not against the reader's own name: one component
        // claiming and another resolving is a shape the format is meant to allow.
        Assert.True(ClaimKey.IsValid("some-other-component/2026/03/07/m1"));
    }

    [Theory]
    // Absolute paths: Path.Combine drops the base entirely for these.
    [InlineData("/etc/passwd")]
    [InlineData("/orders/2026/03/07/m1")]
    // Traversal, at every segment.
    [InlineData("../../etc/passwd")]
    [InlineData("orders/../../../etc/passwd")]
    [InlineData("orders/2026/03/07/../../../../etc/passwd")]
    [InlineData("../2026/03/07/m1")]
    [InlineData("orders/2026/03/07/..")]
    [InlineData("orders/2026/03/07/.")]
    // Separators a segment did not declare.
    [InlineData("orders\\2026\\03\\07\\m1")]
    [InlineData("orders/2026/03/07/sub/m1")]
    // Wrong shape.
    [InlineData("orders/2026/03/m1")]
    [InlineData("orders/2026/03/07/m1/")]
    [InlineData("/")]
    [InlineData("")]
    [InlineData(null)]
    // Date segments that are not dates.
    [InlineData("orders/26/03/07/m1")]
    [InlineData("orders/2026/3/07/m1")]
    [InlineData("orders/2026/aa/07/m1")]
    // An empty segment.
    [InlineData("orders//03/07/m1")]
    public void IsValid_RejectsAnythingItCouldNotHaveMinted(string? dataRef) =>
        Assert.False(ClaimKey.IsValid(dataRef));

    [Theory]
    [InlineData("..")]
    [InlineData(".")]
    [InlineData("orders/extra")]
    [InlineData("orders\\extra")]
    public void For_RefusesASegmentItsOwnReaderWouldReject(string componentName) =>
        Assert.Throws<ArgumentException>(() => ClaimKey.For(componentName, WrittenAt, "m1"));

    [Fact]
    public void For_RefusesAMessageIdItsOwnReaderWouldReject() =>
        Assert.Throws<ArgumentException>(() => ClaimKey.For("orders", WrittenAt, "../../etc/passwd"));
}
