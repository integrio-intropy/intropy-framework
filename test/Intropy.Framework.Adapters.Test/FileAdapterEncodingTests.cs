using System.Text;
using Dapr.Client;
using Intropy.Framework.Adapters.File;
using NSubstitute;

namespace Intropy.Framework.Adapters.Test;

/// <summary>
/// Unit tests for file adapter encoding behavior.
/// These tests verify that file adapters handle different encodings correctly.
/// </summary>
public class FileAdapterEncodingTests
{
    private readonly FileAdapterOptions _options = new("test-binding", "/test");

    #region UTF-8 Encoding Tests

    [Fact]
    public async Task GetContentAsync_Utf8_WithoutBom_ReturnsCorrectContent()
    {
        // Arrange
        const string expectedContent = "Hello, World! åäö";
        var utf8Bytes = Encoding.UTF8.GetBytes(expectedContent);

        var adapter = CreateAdapterWithResponse(utf8Bytes);

        // Act
        var result = await adapter.GetContentAsync("test.txt", Encoding.UTF8);

        // Assert
        Assert.Equal(expectedContent, result);
    }

    [Fact]
    public async Task GetContentAsync_Utf8_WithBom_IncludesBomInContent()
    {
        // Arrange
        const string expectedContent = "Hello, World!";
        var utf8Bom = new byte[] { 0xEF, 0xBB, 0xBF };
        var contentBytes = Encoding.UTF8.GetBytes(expectedContent);
        var bytesWithBom = utf8Bom.Concat(contentBytes).ToArray();

        var adapter = CreateAdapterWithResponse(bytesWithBom);

        // Act
        var result = await adapter.GetContentAsync("test.txt", Encoding.UTF8);

        // Assert
        // NOTE: Standard Encoding.UTF8 does NOT strip BOM - this documents current behavior
        // The result will have the BOM character at the start
        Assert.StartsWith("\uFEFF", result);
        Assert.Contains(expectedContent, result);
    }

    [Fact]
    public async Task GetContentAsync_Utf8_WithSpecialCharacters_ReturnsCorrectContent()
    {
        // Arrange - includes various Unicode characters
        const string expectedContent = "Emoji: 🎉 Chinese: 中文 Arabic: العربية Greek: Ελληνικά";
        var utf8Bytes = Encoding.UTF8.GetBytes(expectedContent);

        var adapter = CreateAdapterWithResponse(utf8Bytes);

        // Act
        var result = await adapter.GetContentAsync("test.txt", Encoding.UTF8);

        // Assert
        Assert.Equal(expectedContent, result);
    }

    [Fact]
    public async Task GetContentAsync_Utf8_WithNewlines_PreservesLineEndings()
    {
        // Arrange - test different line ending styles
        const string contentWithCrLf = "Line1\r\nLine2\r\nLine3";
        const string contentWithLf = "Line1\nLine2\nLine3";
        const string contentWithCr = "Line1\rLine2\rLine3";

        var adapterCrLf = CreateAdapterWithResponse(Encoding.UTF8.GetBytes(contentWithCrLf));
        var adapterLf = CreateAdapterWithResponse(Encoding.UTF8.GetBytes(contentWithLf));
        var adapterCr = CreateAdapterWithResponse(Encoding.UTF8.GetBytes(contentWithCr));

        // Act
        var resultCrLf = await adapterCrLf.GetContentAsync("test.txt", Encoding.UTF8);
        var resultLf = await adapterLf.GetContentAsync("test.txt", Encoding.UTF8);
        var resultCr = await adapterCr.GetContentAsync("test.txt", Encoding.UTF8);

        // Assert
        Assert.Equal(contentWithCrLf, resultCrLf);
        Assert.Equal(contentWithLf, resultLf);
        Assert.Equal(contentWithCr, resultCr);
    }

    #endregion

    #region UTF-16 Encoding Tests

    [Fact]
    public async Task GetContentAsync_Utf16Le_WithBom_ReturnsCorrectContent()
    {
        // Arrange
        const string expectedContent = "Hello UTF-16 LE åäö";
        var encoding = Encoding.Unicode; // UTF-16 LE
        var bytes = encoding.GetPreamble().Concat(encoding.GetBytes(expectedContent)).ToArray();

        var adapter = CreateAdapterWithResponse(bytes);

        // Act
        var result = await adapter.GetContentAsync("test.txt", encoding);

        // Assert
        Assert.Contains("Hello UTF-16 LE", result);
    }

    [Fact]
    public async Task GetContentAsync_Utf16Be_WithBom_ReturnsCorrectContent()
    {
        // Arrange
        const string expectedContent = "Hello UTF-16 BE åäö";
        var encoding = Encoding.BigEndianUnicode; // UTF-16 BE
        var bytes = encoding.GetPreamble().Concat(encoding.GetBytes(expectedContent)).ToArray();

        var adapter = CreateAdapterWithResponse(bytes);

        // Act
        var result = await adapter.GetContentAsync("test.txt", encoding);

        // Assert
        Assert.Contains("Hello UTF-16 BE", result);
    }

    [Fact]
    public async Task GetContentAsync_Utf16Le_ReadAsUtf8_ProducesGarbledContent()
    {
        // Arrange - UTF-16 LE file read with wrong encoding (UTF-8)
        const string originalContent = "Hello";
        var utf16Bytes = Encoding.Unicode.GetBytes(originalContent);

        var adapter = CreateAdapterWithResponse(utf16Bytes);

        // Act
        var result = await adapter.GetContentAsync("test.txt", Encoding.UTF8);

        // Assert - Content will be garbled because UTF-16 bytes interpreted as UTF-8
        Assert.NotEqual(originalContent, result);
    }

    #endregion

    #region Windows-1252 / ISO-8859-1 Encoding Tests

    [Fact]
    public async Task GetContentAsync_Windows1252_WithSpecialCharacters_ReturnsCorrectContent()
    {
        // Arrange
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var windows1252 = Encoding.GetEncoding(1252);
        const string expectedContent = "Ångström café naïve";
        var bytes = windows1252.GetBytes(expectedContent);

        var adapter = CreateAdapterWithResponse(bytes);

        // Act
        var result = await adapter.GetContentAsync("test.txt", windows1252);

        // Assert
        Assert.Equal(expectedContent, result);
    }

    [Fact]
    public async Task GetContentAsync_Windows1252_ReadAsUtf8_MayProduceReplacementCharacters()
    {
        // Arrange - Windows-1252 specific characters that don't exist in UTF-8
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

        // These Windows-1252 bytes (0x80-0x9F range) have special meanings
        // and will not decode correctly as UTF-8
        var bytes = new byte[] { 0x80, 0x81, 0x82 }; // €, undefined, ‚ in Windows-1252

        var adapter = CreateAdapterWithResponse(bytes);

        // Act
        var result = await adapter.GetContentAsync("test.txt", Encoding.UTF8);

        // Assert - UTF-8 cannot decode these bytes correctly
        // Either produces replacement characters or throws
        Assert.NotNull(result);
    }

    [Fact]
    public async Task GetContentAsync_Iso88591_ReturnsCorrectContent()
    {
        // Arrange
        var iso88591 = Encoding.GetEncoding("ISO-8859-1");
        const string expectedContent = "Héllo Wörld";
        var bytes = iso88591.GetBytes(expectedContent);

        var adapter = CreateAdapterWithResponse(bytes);

        // Act
        var result = await adapter.GetContentAsync("test.txt", iso88591);

        // Assert
        Assert.Equal(expectedContent, result);
    }

    [Fact]
    public async Task GetContentAsync_Iso88591_ReadAsUtf8_ProducesIncorrectCharacters()
    {
        // Arrange - ISO-8859-1 extended characters (0x80-0xFF)
        var iso88591 = Encoding.GetEncoding("ISO-8859-1");
        const string originalContent = "café";
        var bytes = iso88591.GetBytes(originalContent);

        var adapter = CreateAdapterWithResponse(bytes);

        // Act - Read with wrong encoding
        var result = await adapter.GetContentAsync("test.txt", Encoding.UTF8);

        // Assert - High bytes will be misinterpreted
        // The 'é' in ISO-8859-1 (0xE9) is not valid single-byte UTF-8
        Assert.NotEqual(originalContent, result);
    }

    #endregion

    #region ASCII Encoding Tests

    [Fact]
    public async Task GetContentAsync_Ascii_ReturnsCorrectContent()
    {
        // Arrange
        const string expectedContent = "Hello World 123";
        var bytes = Encoding.ASCII.GetBytes(expectedContent);

        var adapter = CreateAdapterWithResponse(bytes);

        // Act
        var result = await adapter.GetContentAsync("test.txt", Encoding.ASCII);

        // Assert
        Assert.Equal(expectedContent, result);
    }

    [Fact]
    public async Task GetContentAsync_Ascii_WithHighBytes_LosesData()
    {
        // Arrange - UTF-8 content with non-ASCII characters
        const string originalContent = "Hello åäö";
        var utf8Bytes = Encoding.UTF8.GetBytes(originalContent);

        var adapter = CreateAdapterWithResponse(utf8Bytes);

        // Act - Read as ASCII (which only supports 0-127)
        var result = await adapter.GetContentAsync("test.txt", Encoding.ASCII);

        // Assert - Non-ASCII bytes become '?' or similar
        Assert.Contains("Hello", result);
        Assert.DoesNotContain("åäö", result);
    }

    #endregion

    #region Binary Content Tests

    [Fact]
    public async Task GetContentAsync_Binary_ReturnsExactBytes()
    {
        // Arrange - Binary content (like an image header)
        var binaryContent = new byte[] { 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A }; // PNG header

        var adapter = CreateAdapterWithResponse(binaryContent);

        // Act
        var result = await adapter.GetContentAsync("test.png");

        // Assert
        Assert.Equal(binaryContent, result);
    }

    [Fact]
    public async Task GetContentAsync_BinaryAsString_ProducesUnpredictableResult()
    {
        // Arrange - Binary content read as UTF-8 string
        var binaryContent = new byte[] { 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A };

        var adapter = CreateAdapterWithResponse(binaryContent);

        // Act
        var result = await adapter.GetContentAsync("test.png", Encoding.UTF8);

        // Assert - Binary data interpreted as UTF-8 will have replacement characters or garbage
        Assert.NotNull(result);
        // The 0x89 byte is not valid UTF-8 start byte
    }

    #endregion

    #region Empty and Edge Cases

    [Fact]
    public async Task GetContentAsync_EmptyFile_ReturnsEmptyString()
    {
        // Arrange
        var emptyBytes = Array.Empty<byte>();

        var adapter = CreateAdapterWithResponse(emptyBytes);

        // Act
        var result = await adapter.GetContentAsync("empty.txt", Encoding.UTF8);

        // Assert
        Assert.Equal(string.Empty, result);
    }

    [Fact]
    public async Task GetContentAsync_EmptyFile_ReturnsEmptyByteArray()
    {
        // Arrange
        var emptyBytes = Array.Empty<byte>();

        var adapter = CreateAdapterWithResponse(emptyBytes);

        // Act
        var result = await adapter.GetContentAsync("empty.txt");

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetContentAsync_NullBytes_InContent_PreservesNulls()
    {
        // Arrange - Content with embedded null bytes
        var contentWithNulls = new byte[] { 0x48, 0x00, 0x65, 0x00, 0x6C, 0x00 }; // "H\0e\0l\0"

        var adapter = CreateAdapterWithResponse(contentWithNulls);

        // Act
        var result = await adapter.GetContentAsync("test.txt");

        // Assert
        Assert.Equal(contentWithNulls, result);
    }

    [Fact]
    public async Task GetContentAsync_VeryLargeFile_HandlesCorrectly()
    {
        // Arrange - 1MB of text
        var largeContent = new string('A', 1024 * 1024);
        var bytes = Encoding.UTF8.GetBytes(largeContent);

        var adapter = CreateAdapterWithResponse(bytes);

        // Act
        var result = await adapter.GetContentAsync("large.txt", Encoding.UTF8);

        // Assert
        Assert.Equal(largeContent.Length, result!.Length);
        Assert.Equal(largeContent, result);
    }

    #endregion

    #region Write/Read Roundtrip Tests

    [Theory]
    [InlineData("UTF-8", "Hello World åäö 中文")]
    [InlineData("UTF-16", "Hello World åäö 中文")]
    [InlineData("ISO-8859-1", "Hello World")]
    [InlineData("ASCII", "Hello World")]
    public async Task WriteAndRead_Roundtrip_PreservesContent(string encodingName, string content)
    {
        // Arrange
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var encoding = Encoding.GetEncoding(encodingName);

        // For this test, we need to capture what was written and return it when read
        byte[]? writtenBytes = null;

        var mockClient = Substitute.For<DaprClient>();
        mockClient.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var request = callInfo.ArgAt<BindingRequest>(0);
                if (request.Operation == "create")
                {
                    writtenBytes = request.Data.ToArray();
                    return Task.FromResult(new BindingResponse(request, Array.Empty<byte>(), new Dictionary<string, string>()));
                }
                if (request.Operation == "get")
                {
                    return Task.FromResult(new BindingResponse(request, writtenBytes ?? [], new Dictionary<string, string>()));
                }
                return Task.FromResult(new BindingResponse(request, Array.Empty<byte>(), new Dictionary<string, string>()));
            });

        var adapter = new SftpAdapter(mockClient, _options);

        // Act
        await adapter.WriteAsync("test.txt", content, encoding);
        var readContent = await adapter.GetContentAsync("test.txt", encoding);

        // Assert
        Assert.Equal(content, readContent);
    }

    #endregion

    #region Mixed Encoding Scenarios

    [Fact]
    public async Task GetContentAsync_Utf8FileWithMixedContent_HandlesCorrectly()
    {
        // Arrange - realistic file with headers, data, and special chars
        var content = """
            # Configuration File
            name=Test Configuration
            description=This file contains åäö and émojis 🎉
            value=12345
            """;
        var bytes = Encoding.UTF8.GetBytes(content);

        var adapter = CreateAdapterWithResponse(bytes);

        // Act
        var result = await adapter.GetContentAsync("config.txt", Encoding.UTF8);

        // Assert
        Assert.Equal(content, result);
        Assert.Contains("åäö", result);
        Assert.Contains("🎉", result);
    }

    [Fact]
    public async Task GetContentAsync_CsvWithDifferentEncodings_DocumentsBehavior()
    {
        // Arrange - CSV content that might come from Excel (often Windows-1252)
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var windows1252 = Encoding.GetEncoding(1252);

        var csvContent = "Name;City;Country\nJöhn;Malmö;Sweden\nMüller;München;Germany";
        var bytes = windows1252.GetBytes(csvContent);

        var adapter = CreateAdapterWithResponse(bytes);

        // Act - Reading with correct encoding
        var correctResult = await adapter.GetContentAsync("data.csv", windows1252);

        // Re-create adapter for second read
        adapter = CreateAdapterWithResponse(bytes);

        // Act - Reading with wrong encoding (common mistake)
        var wrongResult = await adapter.GetContentAsync("data.csv", Encoding.UTF8);

        // Assert
        Assert.Equal(csvContent, correctResult);
        Assert.NotEqual(csvContent, wrongResult); // Wrong encoding produces different result
    }

    #endregion

    #region Real-World File Tests

    [Fact]
    public async Task GetContentAsync_AgressoFile_ReadAsUtf8_ProducesGarbledSwedishCharacters()
    {
        // Arrange - This simulates the actual Agresso file which is ISO-8859-1 encoded
        // The file contains "Lancea Gräsmattan AB" but the 'ä' (0xE4 in ISO-8859-1)
        // is not valid UTF-8, so it gets garbled when read with wrong encoding
        var iso88591 = Encoding.GetEncoding("ISO-8859-1");
        var fileContent = "1IB7                  Lancea Gräsmattan AB";
        var iso88591Bytes = iso88591.GetBytes(fileContent);

        var adapter = CreateAdapterWithResponse(iso88591Bytes);

        // Act - Read with WRONG encoding (UTF-8) - this is likely what's happening in production
        var resultUtf8 = await adapter.GetContentAsync("test.txt", Encoding.UTF8);

        // Assert - The Swedish 'ä' character is garbled
        Assert.DoesNotContain("Gräsmattan", resultUtf8);
        // UTF-8 decoder replaces invalid byte 0xE4 with replacement character
        Assert.Contains("Gr", resultUtf8); // Partial match still works
    }

    [Fact]
    public async Task GetContentAsync_AgressoFile_ReadAsIso88591_ReturnsCorrectSwedishCharacters()
    {
        // Arrange - Same file, correct encoding
        var iso88591 = Encoding.GetEncoding("ISO-8859-1");
        var fileContent = "1IB7                  Lancea Gräsmattan AB";
        var iso88591Bytes = iso88591.GetBytes(fileContent);

        var adapter = CreateAdapterWithResponse(iso88591Bytes);

        // Act - Read with CORRECT encoding
        var result = await adapter.GetContentAsync("test.txt", iso88591);

        // Assert - Swedish characters preserved correctly
        Assert.Contains("Gräsmattan", result);
    }

    [Fact]
    public async Task GetContentAsync_AgressoFile_ReadAsWindows1252_ReturnsCorrectSwedishCharacters()
    {
        // Arrange - Windows-1252 is a superset of ISO-8859-1 for common characters
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var windows1252 = Encoding.GetEncoding(1252);
        var iso88591 = Encoding.GetEncoding("ISO-8859-1");

        var fileContent = "1IB7                  Lancea Gräsmattan AB";
        var iso88591Bytes = iso88591.GetBytes(fileContent);

        var adapter = CreateAdapterWithResponse(iso88591Bytes);

        // Act - Windows-1252 can also read ISO-8859-1 for these characters
        var result = await adapter.GetContentAsync("test.txt", windows1252);

        // Assert
        Assert.Contains("Gräsmattan", result);
    }

    [Fact]
    public async Task GetContentAsync_BinaryRead_ThenManualDecode_WorksCorrectly()
    {
        // Arrange - Demonstrates the safe pattern: read as bytes, then decode
        var iso88591 = Encoding.GetEncoding("ISO-8859-1");
        var fileContent = "1IB7                  Lancea Gräsmattan AB";
        var iso88591Bytes = iso88591.GetBytes(fileContent);

        var adapter = CreateAdapterWithResponse(iso88591Bytes);

        // Act - Read as raw bytes first (safe - no encoding assumption)
        var bytes = await adapter.GetContentAsync("test.txt");

        // Then decode with correct encoding
        var result = iso88591.GetString(bytes);

        // Assert
        Assert.Contains("Gräsmattan", result);
    }

    #endregion

    #region Helper Methods

    private SftpAdapter CreateAdapterWithResponse(byte[] responseData)
    {
        var mockClient = Substitute.For<DaprClient>();
        mockClient.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var request = callInfo.ArgAt<BindingRequest>(0);
                return Task.FromResult(new BindingResponse(request, responseData, new Dictionary<string, string>()));
            });

        return new SftpAdapter(mockClient, _options);
    }

    #endregion
}
