using System.Text;
using Intropy.Framework.Testing.Adapters;

namespace Intropy.Framework.Testing.Test.Adapters;

public class InMemoryFileAdapterTests
{
    [Fact]
    public async Task Sweep_ListGetDelete_BehavesConsistentlyAgainstSameStore()
    {
        var adapter = new InMemoryFileAdapter()
            .AddFile("a.txt", "alpha")
            .AddFile("b.txt", "bravo");

        var listed = await adapter.ListAsync();
        Assert.Equal(2, listed.Count);

        foreach (var file in listed)
        {
            var content = await adapter.GetContentAsync(file.FileName);
            Assert.NotEmpty(content);
            await adapter.DeleteAsync(file.FileName);
        }

        Assert.Empty(await adapter.ListAsync());
        Assert.Empty(adapter.Files);
    }

    [Fact]
    public async Task GetContentAsync_MissingFile_ThrowsFileNotFoundException()
    {
        var adapter = new InMemoryFileAdapter();

        await Assert.ThrowsAsync<FileNotFoundException>(Throws);
        return;

        Task<byte[]> Throws() => adapter.GetContentAsync("missing.txt");
    }

    [Fact]
    public async Task GetContentAsync_WithEncoding_MissingFile_ThrowsFileNotFoundException()
    {
        var adapter = new InMemoryFileAdapter();

        await Assert.ThrowsAsync<FileNotFoundException>(Throws);
        return;

        async Task<string?> Throws() => await adapter.GetContentAsync("missing.txt", Encoding.UTF8);
    }

    [Fact]
    public async Task GetContentAsync_WithEncoding_DecodesWithGivenEncoding()
    {
        var adapter = new InMemoryFileAdapter()
            .AddFile("data.txt", Encoding.Unicode.GetBytes("unicode content"));

        var content = await adapter.GetContentAsync("data.txt", Encoding.Unicode);

        Assert.Equal("unicode content", content);
    }

    [Fact]
    public async Task DeleteAsync_MissingFile_NoOps()
    {
        var adapter = new InMemoryFileAdapter();

        await adapter.DeleteAsync("missing.txt");

        Assert.Empty(adapter.Files);
    }

    [Fact]
    public async Task WriteAsync_WithBasePathOverride_StoredUnderEffectivePath()
    {
        var adapter = new InMemoryFileAdapter();

        await adapter.WriteAsync("out.txt", "payload", Encoding.UTF8, basePathOverride: "archive");

        var key = Assert.Single(adapter.Files).Key;
        Assert.Equal("archive/out.txt", key);
        Assert.Equal("payload", adapter.GetString("archive", "out.txt"));
    }

    [Fact]
    public async Task WriteAsync_WithoutOverride_StoredUnderPlainName()
    {
        var adapter = new InMemoryFileAdapter();

        await adapter.WriteAsync("plain.txt", "payload", Encoding.UTF8);

        Assert.Equal("payload", adapter.GetString("plain.txt"));
    }

    [Fact]
    public async Task GetString_MissingFile_ThrowsFileNotFoundException()
    {
        var adapter = new InMemoryFileAdapter();

        Assert.Throws<FileNotFoundException>(() => adapter.GetString("missing.txt"));
    }

    [Fact]
    public async Task Keys_AreOrdinalIgnoreCase()
    {
        var adapter = new InMemoryFileAdapter().AddFile("Report.CSV", "data");

        var content = await adapter.GetContentAsync("report.csv");

        Assert.Equal("data", Encoding.UTF8.GetString(content));
    }

    [Fact]
    public async Task ReadException_ThrowsFromListAndGet_RecoversWhenCleared()
    {
        var adapter = new InMemoryFileAdapter
        {
            ReadException = new InvalidOperationException("source is dead"),
        }.AddFile("a.txt", "alpha");

        await Assert.ThrowsAsync<InvalidOperationException>(adapter.ListAsync);
        await Assert.ThrowsAsync<InvalidOperationException>(GetThrows);

        adapter.ReadException = null;

        Assert.Single(await adapter.ListAsync());
        Assert.NotEmpty(await adapter.GetContentAsync("a.txt"));
        return;

        Task<byte[]> GetThrows() => adapter.GetContentAsync("a.txt");
    }

    [Fact]
    public async Task WriteException_ThrowsFromWrite_RecoversWhenCleared()
    {
        var adapter = new InMemoryFileAdapter
        {
            WriteException = new InvalidOperationException("destination is dead"),
        };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => adapter.WriteAsync("out.txt", "data", Encoding.UTF8));
        Assert.Empty(adapter.Files);

        adapter.WriteException = null;

        await adapter.WriteAsync("out.txt", "data", Encoding.UTF8);
        Assert.Equal("data", adapter.GetString("out.txt"));
    }

    [Fact]
    public async Task Files_ReturnsSnapshot_UnaffectedByLaterMutations()
    {
        var adapter = new InMemoryFileAdapter().AddFile("a.txt", "alpha");

        var snapshot = adapter.Files;
        await adapter.DeleteAsync("a.txt");

        Assert.Single(snapshot);
        Assert.Empty(adapter.Files);
    }
}
