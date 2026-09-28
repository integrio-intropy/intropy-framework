using System.Text;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Hosting.Test.FileSweeps;

/// <summary>
/// The component-neutral file sweep: a source file is completed (deleted, archived, or by a custom
/// completion) only after its handler reports it consumed or a duplicate; a failed, throwing, or
/// uncompletable file stays for the next run and is counted. The extractor's own sweep tests
/// cover cancellation and cleanup through the same code.
/// </summary>
public class FileSweepTests
{
    private const string SourceKey = "source";

    [Fact]
    public async Task SweepAsync_WithConsumedFile_DeletesItOnlyAfterTheHandlerReturns()
    {
        var source = new InMemoryFileAdapter().AddFile("order.json", "{}");
        var presentWhileHandling = false;

        var summary = await Sweep(source).SweepAsync((file, _) =>
        {
            presentWhileHandling = source.Files.ContainsKey(file.Name);
            return Task.FromResult(FileOutcome.Consumed);
        }, CancellationToken.None);

        Assert.True(presentWhileHandling);
        Assert.Empty(source.Files);
        Assert.Equal(1, summary.Processed);
    }

    [Fact]
    public async Task SweepAsync_WithDuplicate_CompletesTheFileAndCountsItSkipped()
    {
        var source = new InMemoryFileAdapter().AddFile("order.json", "{}");

        var summary = await Sweep(source).SweepAsync((_, _) => Task.FromResult(FileOutcome.Duplicate), CancellationToken.None);

        Assert.Empty(source.Files);
        Assert.Equal(1, summary.Skipped);
        Assert.Equal(0, summary.Failed);
    }

    [Fact]
    public async Task SweepAsync_WithFailedOrThrowingHandler_KeepsTheFilesAndCountsThem()
    {
        var source = new InMemoryFileAdapter().AddFile("failed.json", "{}").AddFile("throws.json", "{}");

        var summary = await Sweep(source).SweepAsync((file, _) => file.Name == "throws.json"
            ? throw new InvalidOperationException("broken handler")
            : Task.FromResult(FileOutcome.Failed), CancellationToken.None);

        Assert.Equal(2, source.Files.Count);
        Assert.Equal(2, summary.Failed);
    }

    [Fact]
    public async Task SweepAsync_WithArchiveCompletion_ArchivesTheContentThenDeletesTheSource()
    {
        var source = new InMemoryFileAdapter().AddFile("order.json", "{\"id\":1}");

        var summary = await Sweep(source, FileCompletion.Archive("archive"))
            .SweepAsync(async (file, _) =>
            {
                Assert.Equal("{\"id\":1}", Encoding.UTF8.GetString(await file.ReadAsync(_)));
                return FileOutcome.Consumed;
            }, CancellationToken.None);

        Assert.Equal(1, summary.Processed);
        Assert.False(source.Files.ContainsKey("order.json"));
        Assert.Equal("{\"id\":1}", source.GetString("archive", "order.json"));
    }

    [Fact]
    public async Task SweepAsync_WithArchiveCompletion_ArchivesUnreadContentToo()
    {
        var source = new InMemoryFileAdapter().AddFile("order.json", "{\"id\":2}");

        await Sweep(source, FileCompletion.Archive("archive"))
            .SweepAsync((_, _) => Task.FromResult(FileOutcome.Duplicate), CancellationToken.None);

        Assert.Equal("{\"id\":2}", source.GetString("archive", "order.json"));
    }

    [Fact]
    public async Task SweepAsync_WhenArchivingFails_KeepsTheSourceAndCountsAFailure()
    {
        var source = new InMemoryFileAdapter().AddFile("order.json", "{}");
        source.WriteException = new IOException("archive unavailable");

        var summary = await Sweep(source, FileCompletion.Archive("archive"))
            .SweepAsync((_, _) => Task.FromResult(FileOutcome.Consumed), CancellationToken.None);

        Assert.Equal(1, summary.Failed);
        Assert.True(source.Files.ContainsKey("order.json"));
    }

    [Fact]
    public async Task SweepAsync_WithCustomCompletion_CompletesEachHandledFileWithItsOutcome()
    {
        var source = new InMemoryFileAdapter()
            .AddFile("new.json", "{}").AddFile("duplicate.json", "{}").AddFile("failed.json", "{}");
        var completion = new RecordingCompletion();

        await Sweep(source, completion).SweepAsync((file, _) => Task.FromResult(file.Name switch
        {
            "new.json" => FileOutcome.Consumed,
            "duplicate.json" => FileOutcome.Duplicate,
            _ => FileOutcome.Failed
        }), CancellationToken.None);

        Assert.Equal([("new.json", FileOutcome.Consumed), ("duplicate.json", FileOutcome.Duplicate)],
            completion.Completed);
        Assert.Equal(3, source.Files.Count); // this completion leaves files in place
    }

    [Fact]
    public async Task SweepAsync_WhenCustomCompletionThrows_KeepsTheSourceAndCountsAFailure()
    {
        var source = new InMemoryFileAdapter().AddFile("order.json", "{}");

        var summary = await Sweep(source, new ThrowingCompletion())
            .SweepAsync((_, _) => Task.FromResult(FileOutcome.Consumed), CancellationToken.None);

        Assert.Equal(0, summary.Processed);
        Assert.Equal(1, summary.Failed);
        Assert.True(source.Files.ContainsKey("order.json"));
    }

    [Fact]
    public async Task SweepAsync_GivesEachFileItsOwnScope()
    {
        var source = new InMemoryFileAdapter().AddFile("a.json", "{}").AddFile("b.json", "{}");
        var scopes = new List<ScopedMarker>();

        await Sweep(source, services: s => s.AddScoped<ScopedMarker>())
            .SweepAsync((file, _) =>
            {
                scopes.Add(file.Services.GetRequiredService<ScopedMarker>());
                return Task.FromResult(FileOutcome.Consumed);
            }, CancellationToken.None);

        Assert.Equal(2, scopes.Count);
        Assert.NotSame(scopes[0], scopes[1]);
    }

    [Fact]
    public async Task SweepAsync_RecordsEachFileByOutcome()
    {
        using var metrics = new MetricCapture();
        var port = $"metrics-port-{Guid.NewGuid()}";
        var source = new InMemoryFileAdapter()
            .AddFile("consumed.json", "{}").AddFile("duplicate.json", "{}").AddFile("failed.json", "{}");
        var collection = new ServiceCollection();
        collection.AddKeyedSingleton<IFileAdapter>(port, source);
        var sweep = new FileSweep(collection.BuildServiceProvider(), port, "sweep-test", FileCompletion.Delete,
            NullLoggerFactory.Instance);

        await sweep.SweepAsync((file, _) => Task.FromResult(file.Name switch
        {
            "consumed.json" => FileOutcome.Consumed,
            "duplicate.json" => FileOutcome.Duplicate,
            _ => FileOutcome.Failed
        }), CancellationToken.None);

        var files = metrics.Of("intropy.sweep.files", "intropy.source.port", port);
        Assert.Equal(["consumed", "duplicate", "failed"], files.Select(f => (string)f.Tags["intropy.sweep.outcome"]!).Order());
        Assert.All(files, f =>
        {
            Assert.Equal(1, f.Value);
            Assert.Equal("sweep-test", f.Tags["intropy.component.name"]);
        });
    }

    private static FileSweep Sweep(
        InMemoryFileAdapter source,
        FileCompletion? completion = null,
        Action<IServiceCollection>? services = null)
    {
        var collection = new ServiceCollection();
        collection.AddKeyedSingleton<IFileAdapter>(SourceKey, source);
        services?.Invoke(collection);
        var provider = collection.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        return new FileSweep(provider, SourceKey, "sweep-test", completion ?? FileCompletion.Delete, NullLoggerFactory.Instance);
    }

    private sealed class ScopedMarker;

    private sealed class RecordingCompletion : FileCompletion
    {
        public List<(string, FileOutcome)> Completed { get; } = [];

        public override Task CompleteAsync(IFileAdapter source, SweptFile file, FileOutcome outcome)
        {
            Completed.Add((file.Name, outcome));
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingCompletion : FileCompletion
    {
        public override Task CompleteAsync(IFileAdapter source, SweptFile file, FileOutcome outcome) =>
            throw new IOException("target unavailable");
    }
}
