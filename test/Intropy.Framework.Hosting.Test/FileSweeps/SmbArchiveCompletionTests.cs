using System.Text.RegularExpressions;
using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.FileSweeps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.FileSweeps;

/// <summary>
/// Archiving from an SMB source moves the file with the binding's rename rather than copying it.
/// A rename never overwrites, so a name the archive already holds is archived under a timestamped
/// name instead of the file staying in the source to be handled on every run.
/// </summary>
public class SmbArchiveCompletionTests
{
    private const string SourceKey = "source";

    [Fact]
    public async Task Archive_RenamesTheFileIntoTheArchive_WithoutRewritingIt()
    {
        var requests = new List<BindingRequest>();
        using var client = Client(requests, refusedRenames: 0);

        var summary = await Sweep(client).SweepAsync((_, _) => Task.FromResult(FileOutcome.Consumed), CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.Equal(["list", "rename"], requests.Select(r => r.Operation));
        Assert.Equal(("/xmlout/order.xml", "/archive/order.xml"),
            (requests[1].Metadata["fileName"], requests[1].Metadata["newFileName"]));
    }

    [Fact]
    public async Task Archive_WhenTheArchiveHasTheName_ArchivesUnderATimestampedName()
    {
        var requests = new List<BindingRequest>();
        using var client = Client(requests, refusedRenames: 1);

        var summary = await Sweep(client).SweepAsync((_, _) => Task.FromResult(FileOutcome.Duplicate), CancellationToken.None);

        Assert.Equal(1, summary.Skipped);
        Assert.Equal(["list", "rename", "rename"], requests.Select(r => r.Operation));
        Assert.Matches(new Regex(@"^/archive/order_\d{8}T\d{9}Z\.xml$", RegexOptions.None, TimeSpan.FromSeconds(1)),
            requests[2].Metadata["newFileName"]);
    }

    [Fact]
    public async Task Archive_WhenBothRenamesFail_KeepsTheSourceAndCountsAFailure()
    {
        var requests = new List<BindingRequest>();
        using var client = Client(requests, refusedRenames: 2);

        var summary = await Sweep(client).SweepAsync((_, _) => Task.FromResult(FileOutcome.Consumed), CancellationToken.None);

        Assert.Equal(1, summary.Failed);
        Assert.DoesNotContain(requests, r => r.Operation is "delete" or "create");
    }

    private static FileSweep Sweep(DaprClient client)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IFileAdapter>(SourceKey, new SmbAdapter(client, new FileAdapterOptions("astro", "/xmlout")));
        var provider = services.BuildServiceProvider();
        return new FileSweep(provider, SourceKey, "sweep-test", FileCompletion.Archive("/archive"), NullLoggerFactory.Instance);
    }

    /// <summary>A Dapr client listing one file, whose first <paramref name="refusedRenames"/>
    /// renames are refused.</summary>
    private static DaprClient Client(List<BindingRequest> requests, int refusedRenames)
    {
        var client = Substitute.For<DaprClient>();
        client.InvokeBindingAsync(Arg.Any<BindingRequest>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var request = call.Arg<BindingRequest>();
                requests.Add(request);
                if (request.Operation == "rename" && refusedRenames-- > 0)
                    return Task.FromException<BindingResponse>(new IOException("rename refused: target exists"));
                var data = request.Operation == "list"
                    ? """[{"fileName":"order.xml","isDirectory":false}]"""u8.ToArray()
                    : [];
                return Task.FromResult(new BindingResponse(request, data, new Dictionary<string, string>()));
            });
        return client;
    }
}
