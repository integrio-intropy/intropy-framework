using System.Text;
using System.Text.RegularExpressions;
using Dapr.Client;
using Intropy.Framework.Adapters.File;
using NSubstitute;

namespace Intropy.Framework.Adapters.Test;

public class FileAdapterRequestTests
{
    private static readonly string[] ExpectedOperations = ["list", "get", "get", "create", "delete"];
    [Theory]
    [InlineData("Local")]
    [InlineData("Sftp")]
    [InlineData("AzureBlob")]
    public async Task ManualAdapters_PreserveProtocolPathsEncodingAndEmptyContent(string kind)
    {
        using var client = Substitute.For<DaprClient>();
        var requests = new List<BindingRequest>();
        client.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var request = call.Arg<BindingRequest>();
                requests.Add(request);
                var json = kind switch
                {
                    "Local" => "[\"/inbound/order.json\",\"/inbound/skip.txt\"]",
                    "Sftp" => "[{\"fileName\":\"order.json\",\"isDirectory\":false},{\"fileName\":\"skip.json\",\"isDirectory\":true}]",
                    _ => "[\"order.json\",{\"Name\":\"skip.txt\"}]"
                };
                return new BindingResponse(request, request.Operation == "list" ? Encoding.UTF8.GetBytes(json) : [], new Dictionary<string, string>());
            });
        var adapter = Create(kind, client, new FileAdapterOptions("orders-source", "/inbound",
            new Regex(@"\.json$", RegexOptions.None, TimeSpan.FromSeconds(1))));

        Assert.Equal("order.json", Assert.Single(await adapter.ListAsync()).Name);
        Assert.Empty(await adapter.GetContentAsync("order.json"));

        // An empty file reads as string.Empty — uniformly, on every adapter kind.
        Assert.Equal(string.Empty, await adapter.GetContentAsync("order.json", Encoding.UTF8));
        await adapter.WriteAsync("order.json", "åäö", Encoding.Unicode, "/override");
        await adapter.DeleteAsync("order.json");

        Assert.All(requests, r => Assert.Equal("orders-source", r.BindingName));
        Assert.Equal(ExpectedOperations, requests.Select(r => r.Operation));
        var pathKey = kind == "AzureBlob" ? "blobName" : "fileName";
        Assert.Equal(kind == "AzureBlob" ? "inbound/order.json" : "/inbound/order.json", requests[1].Metadata[pathKey]);
        Assert.Equal(kind == "AzureBlob" ? "override/order.json" : "/override/order.json", requests[3].Metadata[pathKey]);
        Assert.Equal(Encoding.Unicode.GetBytes("åäö"), requests[3].Data.ToArray());
        if (kind == "AzureBlob")
        {
            Assert.Empty(requests[0].Metadata);
            Assert.Equal("{\"prefix\":\"inbound/\"}", Encoding.UTF8.GetString(requests[0].Data.Span));
            Assert.Equal("include", requests[4].Metadata["deleteSnapshots"]);
        }
        else
        {
            Assert.Equal("/inbound", requests[0].Metadata["fileName"]);
            Assert.Single(requests[4].Metadata);
        }
    }

    [Theory]
    [InlineData("Local")]
    [InlineData("Sftp")]
    [InlineData("AzureBlob")]
    public async Task BindingExceptions_PropagateUnchanged(string kind)
    {
        using var client = Substitute.For<DaprClient>();
        var exception = new IOException("binding unavailable");
        client.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<BindingResponse>(exception));
        var adapter = Create(kind, client, new FileAdapterOptions("source"));
        Assert.Same(exception, await Assert.ThrowsAsync<IOException>(() => adapter.ListAsync()));
        Assert.Same(exception, await Assert.ThrowsAsync<IOException>(() => adapter.GetContentAsync("order.json")));
        Assert.Same(exception, await Assert.ThrowsAsync<IOException>(() => adapter.WriteAsync("order.json", new byte[] { 1 })));
        Assert.Same(exception, await Assert.ThrowsAsync<IOException>(() => adapter.DeleteAsync("order.json")));
    }

    internal static IFileAdapter Create(string kind, DaprClient client, FileAdapterOptions options) => kind switch
    {
        "Local" => new LocalFileAdapter(client, options),
        "Sftp" => new SftpAdapter(client, options),
        "AzureBlob" => new AzureBlobStorageAdapter(client, options),
        _ => throw new ArgumentException("Unsupported test kind", nameof(kind))
    };
}
