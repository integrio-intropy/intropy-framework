using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Blocks.Test.Shared;

public class ArchiveLinkTests
{
    private const string Key = "orders/2026/10/28/order.xml_4bf92f3577b34da6a3ce929d0e0e4736";

    [Theory]
    [InlineData("https://examplearchive.blob.core.windows.net/archive/")]
    [InlineData("https://examplearchive.blob.core.windows.net/archive")]
    public void FromBaseUrl_AppendsTheKey(string baseUrl)
    {
        var link = ArchiveLink.FromBaseUrl(new Uri(baseUrl));

        Assert.Equal("https://examplearchive.blob.core.windows.net/archive/" + Key, link.For(Key));
    }

    [Fact]
    public void FromBaseUrl_RefusesARelativeUrl()
    {
        Assert.Throws<ArgumentException>(() => ArchiveLink.FromBaseUrl(new Uri("archive/", UriKind.Relative)));
    }

    // The shape of an Azure portal blob link: the container and key as one encoded path segment.
    [Fact]
    public void FromTemplate_EncodedKey_EncodesTheSlashes()
    {
        var link = ArchiveLink.FromTemplate(
            "https://portal.example.com/#view/BlobProperties/path/archive%2F{key:encoded}/isDeleted~/false");

        Assert.Equal(
            "https://portal.example.com/#view/BlobProperties/path/archive%2Forders%2F2026%2F10%2F28%2Forder.xml_4bf92f3577b34da6a3ce929d0e0e4736/isDeleted~/false",
            link.For(Key));
    }

    [Fact]
    public void FromTemplate_Key_KeepsTheKeyAsIs()
    {
        var link = ArchiveLink.FromTemplate("https://viewer.example.com/archive/{key}?download=false");

        Assert.Equal($"https://viewer.example.com/archive/{Key}?download=false", link.For(Key));
    }

    [Fact]
    public void FromTemplate_FillsBothPlaceholders()
    {
        var link = ArchiveLink.FromTemplate("https://viewer.example.com/{key}?id={key:encoded}");

        Assert.Equal($"https://viewer.example.com/{Key}?id={Uri.EscapeDataString(Key)}", link.For(Key));
    }

    [Theory]
    [InlineData("https://viewer.example.com/archive/")]
    [InlineData("https://viewer.example.com/{KEY}")]
    public void FromTemplate_RefusesATemplateWithoutAPlaceholder(string template)
    {
        var error = Assert.Throws<ArgumentException>(() => ArchiveLink.FromTemplate(template));

        Assert.Contains("{key}", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FromTemplate_RefusesATemplateThatIsNotAnAbsoluteUrl()
    {
        var error = Assert.Throws<ArgumentException>(() => ArchiveLink.FromTemplate("archive/{key}"));

        Assert.Contains("absolute", error.Message, StringComparison.Ordinal);
    }
}
