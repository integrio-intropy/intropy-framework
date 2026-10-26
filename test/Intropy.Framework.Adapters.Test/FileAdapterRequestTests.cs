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
    [InlineData("Smb")]
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
                    "Sftp" or "Smb" => "[{\"fileName\":\"order.json\",\"isDirectory\":false},{\"fileName\":\"skip.json\",\"isDirectory\":true}]",
                    _ => "[\"inbound/order.json\",{\"Name\":\"inbound/skip.txt\"}]"
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

    /// <summary>bindings.azure.blobstorage returns full blob names — Azure has no directories, so
    /// the prefix is part of the name. The adapter strips the configured prefix so listed names are
    /// relative to the base path, as every other adapter's are.</summary>
    [Theory]
    // A flat blob lists as its bare name, whatever slash edges the base path was written with.
    [InlineData("inbound", "[\"inbound/order.json\"]", "order.json")]
    [InlineData("/inbound/", "[\"inbound/order.json\"]", "order.json")]
    // A nested blob keeps its relative path, for CombinePath to re-join.
    [InlineData("inbound", "[\"inbound/2026/order.json\"]", "2026/order.json")]
    // No base path configured: nothing to strip.
    [InlineData("", "[\"order.json\"]", "order.json")]
    // A name outside the configured prefix is passed through rather than silently dropped.
    [InlineData("inbound", "[\"other/order.json\"]", "other/order.json")]
    public async Task BlobListing_StripsTheConfiguredPrefix(string basePath, string listJson, string expected)
    {
        var requests = new List<BindingRequest>();
        using var client = BlobClient(listJson, requests);
        var adapter = new AzureBlobStorageAdapter(client, new FileAdapterOptions("orders-source", basePath));

        Assert.Equal(expected, Assert.Single(await adapter.ListAsync()).Name);

        // The listed name round-trips: a get on it targets the blob the listing came from,
        // with the base path applied exactly once.
        await adapter.GetContentAsync(expected);
        var expectedBlob = basePath.Trim().Trim('/') is { Length: > 0 } prefix ? $"{prefix}/{expected}" : expected;
        Assert.Equal(expectedBlob, requests[1].Metadata["blobName"]);
    }

    /// <summary>The regression behind the secondary symptom: an anchored FileNameRegex is matched
    /// against the listed name, so it behaves on blob exactly as it does on the other adapters.</summary>
    [Fact]
    public async Task BlobListing_MatchesAnAnchoredRegexAgainstTheRelativeName()
    {
        var requests = new List<BindingRequest>();
        using var client = BlobClient(
            "[\"inbound/orders/2026/ORD-1001.xml\",\"inbound/orders/2026/summary.xml\"]", requests);
        var adapter = new AzureBlobStorageAdapter(client, new FileAdapterOptions(
            "orders-source", "inbound/orders/2026",
            new Regex(@"^ORD-\d+\.xml$", RegexOptions.None, TimeSpan.FromSeconds(1))));

        Assert.Equal("ORD-1001.xml", Assert.Single(await adapter.ListAsync()).Name);
    }

    /// <summary>A zero-length blob standing in for the prefix "directory" strips to nothing and is
    /// not listed as a file.</summary>
    [Fact]
    public async Task BlobListing_DropsADirectoryMarkerBlob()
    {
        var requests = new List<BindingRequest>();
        using var client = BlobClient("[\"inbound/\",{\"Name\":\"inbound/order.json\"}]", requests);
        var adapter = new AzureBlobStorageAdapter(client, new FileAdapterOptions("orders-source", "inbound"));

        Assert.Equal("order.json", Assert.Single(await adapter.ListAsync()).Name);
    }

    /// <summary>A Dapr client whose list operation returns <paramref name="listJson"/> and whose
    /// other operations return no content, recording every request into <paramref name="requests"/>.</summary>
    private static DaprClient BlobClient(string listJson, List<BindingRequest> requests)
    {
        var client = Substitute.For<DaprClient>();
        client.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var request = call.Arg<BindingRequest>();
                requests.Add(request);
                return new BindingResponse(request,
                    request.Operation == "list" ? Encoding.UTF8.GetBytes(listJson) : [],
                    new Dictionary<string, string>());
            });
        return client;
    }

    [Theory]
    [InlineData("Local")]
    [InlineData("Sftp")]
    [InlineData("Smb")]
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
        "Smb" => new SmbAdapter(client, options),
        "AzureBlob" => new AzureBlobStorageAdapter(client, options),
        _ => throw new ArgumentException("Unsupported test kind", nameof(kind))
    };
}
