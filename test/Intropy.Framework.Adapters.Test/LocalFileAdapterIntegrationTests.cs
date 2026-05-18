using System.Text;
using Intropy.Framework.Adapters.File;

namespace Intropy.Framework.Adapters.Test;

/// <summary>
/// Integration tests for <see cref="LocalFileAdapter"/> using Testcontainers.
/// These tests automatically start a Dapr sidecar container with local storage binding.
/// </summary>
[Collection(nameof(DaprContainerCollection))]
[Trait("Category", "Integration")]
public class LocalFileAdapterIntegrationTests : IAsyncLifetime
{
    private readonly DaprContainerFixture _fixture;
    private readonly string _testBasePath;
    private readonly string _testFileName;

    private LocalFileAdapter _adapter = null!;

    public LocalFileAdapterIntegrationTests(DaprContainerFixture fixture)
    {
        _fixture = fixture;
        // Use unique paths per test run to avoid conflicts
        _testBasePath = $"test-{Guid.NewGuid():N}";
        _testFileName = "test-file.txt";
    }

    public Task InitializeAsync()
    {
        var options = new FileAdapterOptions(DaprContainerFixture.BindingName, _testBasePath);
        _adapter = new LocalFileAdapter(_fixture.DaprClient, options);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        // Clean up test files
        try
        {
            var files = await _adapter.ListAsync();
            foreach (var file in files)
            {
                await _adapter.DeleteAsync(file.FileName);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    [Fact]
    public async Task WriteAsync_CreatesFile()
    {
        // Arrange
        var content = "Hello from integration test!"u8.ToArray();

        // Act
        await _adapter.WriteAsync(_testFileName, content);

        // Assert
        var readContent = await _adapter.GetContentAsync(_testFileName);
        Assert.Equal(content, readContent);
    }

    [Fact]
    public async Task WriteAsync_WithStringContent_CreatesFile()
    {
        // Arrange
        const string testContent = "Hello from string test!";

        // Act
        await _adapter.WriteAsync(_testFileName, testContent, Encoding.UTF8);

        // Assert
        var readContent = await _adapter.GetContentAsync(_testFileName, Encoding.UTF8);
        Assert.Equal(testContent, readContent);
    }

    [Fact]
    public async Task GetContentAsync_ReturnsFileContent()
    {
        // Arrange
        var content = "Content to read"u8.ToArray();
        await _adapter.WriteAsync(_testFileName, content);

        // Act
        var result = await _adapter.GetContentAsync(_testFileName);

        // Assert
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task GetContentAsync_WithEncoding_ReturnsStringContent()
    {
        // Arrange
        const string testContent = "String content to read";
        await _adapter.WriteAsync(_testFileName, testContent, Encoding.UTF8);

        // Act
        var result = await _adapter.GetContentAsync(_testFileName, Encoding.UTF8);

        // Assert
        Assert.Equal(testContent, result);
    }

    [Fact]
    public async Task ListAsync_ReturnsFiles()
    {
        // Arrange
        await _adapter.WriteAsync(_testFileName, "list test content", Encoding.UTF8);

        // Act
        var files = await _adapter.ListAsync();

        // Assert
        Assert.Contains(files, f => f.FileName == _testFileName);
    }

    [Fact]
    public async Task ListAsync_WithMultipleFiles_ReturnsAllFiles()
    {
        // Arrange
        await _adapter.WriteAsync("file1.txt", "content1", Encoding.UTF8);
        await _adapter.WriteAsync("file2.txt", "content2", Encoding.UTF8);
        await _adapter.WriteAsync("file3.txt", "content3", Encoding.UTF8);

        // Act
        var files = await _adapter.ListAsync();

        // Assert
        Assert.Equal(3, files.Count);
        Assert.Contains(files, f => f.FileName == "file1.txt");
        Assert.Contains(files, f => f.FileName == "file2.txt");
        Assert.Contains(files, f => f.FileName == "file3.txt");
    }

    [Fact]
    public async Task DeleteAsync_RemovesFile()
    {
        // Arrange
        await _adapter.WriteAsync(_testFileName, "content to delete", Encoding.UTF8);
        var filesBefore = await _adapter.ListAsync();
        Assert.Contains(filesBefore, f => f.FileName == _testFileName);

        // Act
        await _adapter.DeleteAsync(_testFileName);

        // Assert
        var filesAfter = await _adapter.ListAsync();
        Assert.DoesNotContain(filesAfter, f => f.FileName == _testFileName);
    }

    [Fact]
    public async Task WriteAsync_WithBasePathOverride_WritesToOverridePath()
    {
        // Arrange
        var overridePath = $"override-{Guid.NewGuid():N}";
        const string overrideFileName = "override-test.txt";
        const string content = "Override content";

        // Act
        await _adapter.WriteAsync(overrideFileName, content, Encoding.UTF8, overridePath);

        // Assert - create a new adapter with the override path to verify
        var overrideOptions = new FileAdapterOptions(DaprContainerFixture.BindingName, overridePath);
        var overrideAdapter = new LocalFileAdapter(_fixture.DaprClient, overrideOptions);

        var files = await overrideAdapter.ListAsync();
        Assert.Contains(files, f => f.FileName == overrideFileName);

        var readContent = await overrideAdapter.GetContentAsync(overrideFileName, Encoding.UTF8);
        Assert.Equal(content, readContent);

        // Cleanup
        await overrideAdapter.DeleteAsync(overrideFileName);
    }

    [Fact]
    public async Task WriteAsync_OverwritesExistingFile()
    {
        // Arrange
        await _adapter.WriteAsync(_testFileName, "original content", Encoding.UTF8);

        // Act
        await _adapter.WriteAsync(_testFileName, "updated content", Encoding.UTF8);

        // Assert
        var result = await _adapter.GetContentAsync(_testFileName, Encoding.UTF8);
        Assert.Equal("updated content", result);
    }
}
