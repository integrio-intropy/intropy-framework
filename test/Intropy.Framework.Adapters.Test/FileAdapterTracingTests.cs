using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using Dapr.Client;
using Intropy.Framework.Adapters.File;
using NSubstitute;

namespace Intropy.Framework.Adapters.Test;

public class FileAdapterTracingTests
{
    private const string AdapterActivitySource = "Intropy.Framework.Adapters";

    [Theory]
    [InlineData("Local")]
    [InlineData("Sftp")]
    [InlineData("AzureBlob")]
    public async Task FailedOperations_MarkTheirSpansAsErrorsWithTheException(string kind)
    {
        using var client = Substitute.For<DaprClient>();
        client.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<BindingResponse>(new IOException("binding unavailable")));
        var adapter = FileAdapterRequestTests.Create(kind, client, new FileAdapterOptions("source"));

        var spans = await CaptureAdapterSpansAsync(async () =>
        {
            await Assert.ThrowsAsync<IOException>(() => adapter.ListAsync());
            await Assert.ThrowsAsync<IOException>(() => adapter.GetContentAsync("order.json"));
            await Assert.ThrowsAsync<IOException>(() => adapter.WriteAsync("order.json", new byte[] { 1 }));
            await Assert.ThrowsAsync<IOException>(() => adapter.DeleteAsync("order.json"));
        });

        Assert.Equal(4, spans.Count);
        Assert.All(spans, span =>
        {
            Assert.Equal(ActivityStatusCode.Error, span.Status);
            Assert.Equal("binding unavailable", span.StatusDescription);
            Assert.Contains(span.Events, e => e.Name == "exception");
            Assert.False(string.IsNullOrEmpty(span.Source.Version));
            Assert.Equal(typeof(IOException).FullName, span.GetTagItem("error.type"));
        });
    }

    [Theory]
    [InlineData("Local", "local", "/inbound/order.json", "/archive/order.json")]
    [InlineData("Sftp", "sftp", "/inbound/order.json", "/archive/order.json")]
    [InlineData("AzureBlob", "azure_blob", "inbound/order.json", "archive/order.json")]
    public async Task Operations_NameTheirSpansAfterTheOperationAndBinding_AndDescribeTheFile(string kind,
        string adapterKind, string path, string archivePath)
    {
        using var client = Substitute.For<DaprClient>();
        client.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var request = call.Arg<BindingRequest>();
                var data = request.Operation switch
                {
                    "list" => Encoding.UTF8.GetBytes(ListResponse(kind)),
                    "get" => "{\"id\":1}"u8.ToArray(),
                    _ => []
                };
                return new BindingResponse(request, data, new Dictionary<string, string>());
            });
        var adapter = FileAdapterRequestTests.Create(kind, client, new FileAdapterOptions("orders-source", "/inbound",
            new Regex(@"\.json$", RegexOptions.None, TimeSpan.FromSeconds(1))));

        var spans = await CaptureAdapterSpansAsync(async () =>
        {
            await adapter.ListAsync();
            await adapter.GetContentAsync("order.json");
            await adapter.WriteAsync("order.json", new byte[] { 1, 2, 3 }, "/archive");
            await adapter.DeleteAsync("order.json");
        });

        Assert.Equal(["list orders-source", "get orders-source", "create orders-source", "delete orders-source"],
            spans.OrderBy(s => s.StartTimeUtc).Select(s => s.DisplayName));
        Assert.All(spans, span =>
        {
            Assert.Equal(ActivityKind.Internal, span.Kind);
            Assert.Equal(adapterKind, span.GetTagItem("intropy.file.adapter"));
            Assert.Equal("orders-source", span.GetTagItem("intropy.file.binding"));
            Assert.Equal(span.DisplayName.Split(' ')[0], span.GetTagItem("intropy.file.operation"));
        });

        var list = spans.Single(s => s.DisplayName.StartsWith("list", StringComparison.Ordinal));
        Assert.Equal("/inbound", list.GetTagItem("file.directory"));
        Assert.Equal(2, list.GetTagItem("intropy.file.listed"));
        Assert.Equal(1, list.GetTagItem("intropy.file.matched"));

        var get = spans.Single(s => s.DisplayName.StartsWith("get", StringComparison.Ordinal));
        Assert.Equal("order.json", get.GetTagItem("file.name"));
        Assert.Equal(path, get.GetTagItem("file.path"));
        Assert.Equal(8, get.GetTagItem("file.size"));

        var create = spans.Single(s => s.DisplayName.StartsWith("create", StringComparison.Ordinal));
        Assert.Equal(archivePath, create.GetTagItem("file.path"));
        Assert.Equal(3, create.GetTagItem("file.size"));

        var delete = spans.Single(s => s.DisplayName.StartsWith("delete", StringComparison.Ordinal));
        Assert.Equal(path, delete.GetTagItem("file.path"));
    }

    /// <summary>A listing of two files, one of which the <c>.json</c> filter drops.</summary>
    private static string ListResponse(string kind) => kind switch
    {
        "Local" => "[\"/inbound/order.json\",\"/inbound/skip.txt\"]",
        "Sftp" => "[{\"fileName\":\"order.json\",\"isDirectory\":false},{\"fileName\":\"skip.txt\",\"isDirectory\":false}]",
        _ => "[\"order.json\",{\"Name\":\"skip.txt\"}]"
    };

    [Fact]
    public async Task RequestedCancellation_DoesNotMarkTheSpanAsAnError()
    {
        using var client = Substitute.For<DaprClient>();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        client.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromCanceled<BindingResponse>(call.Arg<CancellationToken>()));
        var adapter = FileAdapterRequestTests.Create("Local", client, new FileAdapterOptions("source"));

        var spans = await CaptureAdapterSpansAsync(() =>
            Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.ListAsync(cts.Token)));

        var span = Assert.Single(spans);
        Assert.Equal(ActivityStatusCode.Unset, span.Status);
        Assert.DoesNotContain(span.Events, e => e.Name == "exception");
    }

    /// <summary>Runs <paramref name="act"/> under a test-owned root span and returns the adapter
    /// spans in its trace only, so spans from tests running in parallel are ignored.</summary>
    private static async Task<List<Activity>> CaptureAdapterSpansAsync(Func<Task> act)
    {
        using var testSource = new ActivitySource($"adapter-tracing-test-{Guid.NewGuid()}");
        var stopped = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source == testSource || source.Name == AdapterActivitySource,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (stopped) stopped.Add(activity); }
        };
        ActivitySource.AddActivityListener(listener);

        ActivityTraceId traceId;
        using (var root = testSource.StartActivity("test")!)
        {
            traceId = root.TraceId;
            await act();
        }

        lock (stopped)
            return stopped.Where(a => a.Source.Name == AdapterActivitySource && a.TraceId == traceId).ToList();
    }
}
