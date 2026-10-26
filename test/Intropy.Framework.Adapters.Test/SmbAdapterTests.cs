using System.Text;
using Dapr.Client;
using Intropy.Framework.Adapters.File;
using NSubstitute;

namespace Intropy.Framework.Adapters.Test;

/// <summary>What the SMB adapter adds over the SFTP shape it shares: exclusive reads, a retried
/// write that already landed, the listing's size and time, and the binding's rename.</summary>
public class SmbAdapterTests
{
    private static readonly FileAdapterOptions Options = new("astro", "/xmlin");

    [Fact]
    public async Task List_ReportsTheSizeAndModificationTimeTheBindingReturns()
    {
        var requests = new List<BindingRequest>();
        using var client = Client(requests, list: """
            [{"fileName":"a.xml","isDirectory":false,"size":12,"modTime":"2026-10-26T10:15:00Z"},
             {"fileName":"sub","isDirectory":true,"size":0,"modTime":"2026-10-26T10:00:00Z"},
             {"fileName":"b.xml","isDirectory":false}]
            """);

        var files = await new SmbAdapter(client, Options).ListAsync();

        Assert.Equal(2, files.Count);
        Assert.Equal(("a.xml", 12L, new DateTimeOffset(2026, 10, 26, 10, 15, 0, TimeSpan.Zero)),
            (files[0].Name, files[0].Size, files[0].ModifiedAt));
        Assert.Equal(("b.xml", (long?)null, (DateTimeOffset?)null), (files[1].Name, files[1].Size, files[1].ModifiedAt));
    }

    [Fact]
    public async Task Get_IsExclusive()
    {
        var requests = new List<BindingRequest>();
        using var client = Client(requests);

        await new SmbAdapter(client, Options).GetContentAsync("a.xml");

        Assert.Equal("/xmlin/a.xml", requests[0].Metadata["fileName"]);
        Assert.Equal("true", requests[0].Metadata["exclusive"]);
    }

    [Fact]
    public async Task Write_RefusedButAlreadyThereWithTheSameContent_Succeeds()
    {
        var requests = new List<BindingRequest>();
        using var client = Client(requests, refuseCreate: true, existing: "<a/>"u8.ToArray());

        await new SmbAdapter(client, Options).WriteAsync("a.xml", "<a/>"u8.ToArray());

        Assert.Equal(["create", "get"], requests.Select(r => r.Operation));
        // The read-back is not exclusive: the partner may already be reading what we wrote.
        Assert.Equal("/xmlin/a.xml", requests[1].Metadata["fileName"]);
        Assert.False(requests[1].Metadata.ContainsKey("exclusive"));
    }

    [Fact]
    public async Task Write_RefusedAndThereWithOtherContent_ThrowsTheWritesError()
    {
        using var client = Client([], refuseCreate: true, existing: "<other/>"u8.ToArray());

        var e = await Assert.ThrowsAsync<IOException>(() =>
            new SmbAdapter(client, Options).WriteAsync("a.xml", "<a/>"u8.ToArray()));
        Assert.Equal("create refused", e.Message);
    }

    [Fact]
    public async Task Write_RefusedAndNotThere_ThrowsTheWritesError()
    {
        using var client = Client([], refuseCreate: true);

        var e = await Assert.ThrowsAsync<IOException>(() =>
            new SmbAdapter(client, Options).WriteAsync("a.xml", "<a/>"u8.ToArray()));
        Assert.Equal("create refused", e.Message);
    }

    [Fact]
    public async Task Move_RenamesUnderTheDestination_KeepingOrReplacingTheName()
    {
        var requests = new List<BindingRequest>();
        using var client = Client(requests);
        var adapter = new SmbAdapter(client, Options);

        await adapter.MoveAsync("a.xml", "/archive");
        await adapter.MoveAsync("b.xml", "/archive", "b_1.xml");

        Assert.All(requests, r => Assert.Equal("rename", r.Operation));
        Assert.Equal(("/xmlin/a.xml", "/archive/a.xml"), (requests[0].Metadata["fileName"], requests[0].Metadata["newFileName"]));
        Assert.Equal(("/xmlin/b.xml", "/archive/b_1.xml"), (requests[1].Metadata["fileName"], requests[1].Metadata["newFileName"]));
    }

    /// <summary>A Dapr client recording every request. <c>list</c> answers <paramref name="list"/>;
    /// <c>create</c> throws when <paramref name="refuseCreate"/>; <c>get</c> answers
    /// <paramref name="existing"/>, or throws when there is none.</summary>
    private static DaprClient Client(List<BindingRequest> requests, string list = "[]", bool refuseCreate = false,
        byte[]? existing = null)
    {
        var client = Substitute.For<DaprClient>();
        client.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var request = call.Arg<BindingRequest>();
                requests.Add(request);
                return request.Operation switch
                {
                    "create" when refuseCreate => Task.FromException<BindingResponse>(new IOException("create refused")),
                    "get" when refuseCreate && existing is null => Task.FromException<BindingResponse>(new IOException("not found")),
                    _ => Task.FromResult(new BindingResponse(request, request.Operation switch
                    {
                        "list" => Encoding.UTF8.GetBytes(list),
                        "get" => existing ?? [],
                        _ => []
                    }, new Dictionary<string, string>()))
                };
            });
        return client;
    }
}
