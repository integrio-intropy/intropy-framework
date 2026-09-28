using System.Text;
using Intropy.Framework.Adapters.Common;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.FileSweeps;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.FileSweeps;

public class SweptFileTests
{
    [Fact]
    public async Task ReadTextAsync_FetchesTheContentOnce_AndKeepsItForArchiving()
    {
        // The sweep's contract: content is read once, whatever order the handler and the
        // completion read it in — an archive after a text read must not fetch the file again
        var source = new CountingAdapter("order-1.json"u8.ToArray());
        var file = new SweptFile("order-1.json", source, new ServiceCollection().BuildServiceProvider());

        var text = await file.ReadTextAsync(Encoding.UTF8);
        var archived = await file.ReadAsync();
        var textAgain = await file.ReadTextAsync(Encoding.UTF8);

        Assert.Equal("order-1.json", text);
        Assert.Equal("order-1.json", Encoding.UTF8.GetString(archived));
        Assert.Equal(text, textAgain);
        Assert.Equal(1, source.Fetches);
    }

    [Fact]
    public async Task ReadTextAsync_ReadsAnEmptyFileAsEmpty()
    {
        var source = new CountingAdapter([]);
        var file = new SweptFile("empty.json", source, new ServiceCollection().BuildServiceProvider());

        Assert.Equal(string.Empty, await file.ReadTextAsync(Encoding.UTF8));
    }

    [Fact]
    public async Task ReadAsync_ForwardsCancellationToTheSource()
    {
        var source = new CountingAdapter([]);
        var file = new SweptFile("order-1.json", source, new ServiceCollection().BuildServiceProvider());
        using var cts = new CancellationTokenSource();

        await file.ReadTextAsync(Encoding.UTF8, cts.Token);

        Assert.Equal(cts.Token, source.SeenToken);
    }

    private sealed class CountingAdapter(byte[] content) : IFileAdapter
    {
        public int Fetches { get; private set; }
        public CancellationToken SeenToken { get; private set; }

        public Task<List<FileEntry>> ListAsync(CancellationToken ct = default) => throw new NotSupportedException();

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default)
        {
            Fetches++;
            SeenToken = ct;
            return Task.FromResult(content);
        }

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task DeleteAsync(string fileName, CancellationToken ct = default) => throw new NotSupportedException();
    }
}
