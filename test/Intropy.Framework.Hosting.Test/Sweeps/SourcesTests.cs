using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Sweeps;

namespace Intropy.Framework.Hosting.Test.Sweeps;

/// <summary>
/// A component's sources: file ports declared by name, ports of any kind
/// with <see cref="Sources.Port(SourcePort)"/>, in declaration order, and each port name declared once whatever
/// kind of source it is.
/// </summary>
public class SourcesTests
{
    [Fact]
    public void Port_DeclaresAFileSourceThatDeletesHandledFilesByDefault()
    {
        var sources = new Sources().Port("orders-in");

        var port = Assert.IsType<FileSourcePort>(Assert.Single(sources.Ports));
        Assert.Equal("orders-in", port.Name);
        Assert.Same(FileCompletion.Delete, port.Completion);
    }

    [Fact]
    public void Port_DeclaresPortsOfAnyKindInDeclarationOrder()
    {
        var feed = new ListedSourcePort("feed", []);

        var sources = new Sources().Port("orders-in").Port(feed);

        Assert.Equal(["orders-in", "feed"], sources.Ports.Select(p => p.Name));
        Assert.Same(feed, sources.Ports[1]);
    }

    [Fact]
    public void APortName_IsDeclaredOnceWhateverKindOfSourceItIs()
    {
        var sources = new Sources().Port("orders-in");

        var error = Assert.Throws<ArgumentException>(() => sources.Port(new ListedSourcePort("orders-in", [])));

        Assert.Contains("'orders-in' is already declared", error.Message);
        Assert.Throws<ArgumentException>(() => new Sources().Port(new ListedSourcePort("feed", [])).Port("feed"));
    }

    [Fact]
    public void Port_RejectsANullSourcePort() =>
        Assert.Throws<ArgumentNullException>(() => new Sources().Port((SourcePort)null!));
}
