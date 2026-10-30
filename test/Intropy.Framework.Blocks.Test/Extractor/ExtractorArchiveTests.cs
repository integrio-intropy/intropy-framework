using System.Text;
using CloudNative.CloudEvents;
using Dapr.Client;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Adapters.Common;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.Extractor;

/// <summary>
/// A whole extractor pipeline with an archive configured: which payloads reach the archive store,
/// under which key, and what an incident says about the copy.
/// </summary>
public class ExtractorArchiveTests
{
    private const string ReceivedAt = "2026-10-28T10:15:00.0000000Z";

    private static readonly Uri BrowseBaseUrl = new("https://starchive.blob.core.windows.net/archive/");

    private readonly RecordingStore _store = new();
    private readonly IBusinessIncidentServiceClient _incidents = Substitute.For<IBusinessIncidentServiceClient>();
    private readonly IServiceProvider _provider;
    private int _published;

    public ExtractorArchiveTests()
    {
        var daprClient = Substitute.For<DaprClient>();
        daprClient.PublishByteEventAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(_ => _published++);

        var services = new ServiceCollection();
        services.AddSingleton(_incidents);
        services.AddSingleton(daprClient);
        services.AddLogging();
        services.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "orders";
            conf.ServiceNamespace = "example";
        });
        _provider = services.BuildServiceProvider();
    }

    private Extractor<string, string, Context> Build(ArchiveMode? mode, Uri? browseBaseUrl = null) =>
        ExtractorBuilder<string, string, Context>.Create("orders.Process", _provider)
            .WithDeserializer(new TextDeserializer())
            .WithValidator(new RejectingValidator())
            .WithTransformer(new IdentityTransformer())
            .WithSerializer(new CloudEventSerializeStep<string, Context>(s => s, _ => DateTimeOffset.UtcNow))
            .WithDaprTopicPublisher("pubsub", "orders", new Uri("urn:example:orders"), "com.example.order")
            .WithBusinessIncidents(_ => "orders", _ => "orders")
            .WithArchive(mode is { } m ? new ArchivePolicy("orders", _store, m, browseBaseUrl, TimeProvider.System) : null)
            .Build();

    private static ReadOnlyMemory<byte> Input(string text) => Encoding.Latin1.GetBytes(text);

    // The context a file sweep gives every file.
    private static Context FileContext() => new(new Dictionary<string, string>
    {
        [SourceContextKeys.FileName] = "order 1.xml",
        [SourceContextKeys.ReceivedAt] = ReceivedAt
    });

    private BusinessIncidentData? RoutedIncident()
    {
        var call = _incidents.ReceivedCalls().SingleOrDefault(c => c.GetMethodInfo().Name == nameof(IBusinessIncidentServiceClient.Trigger));
        return call?.GetArguments()[3] as BusinessIncidentData;
    }

    [Fact]
    public async Task OnIncident_AnIncidentsPayloadIsArchivedUnchanged_AndTheIncidentNamesTheCopy()
    {
        // Latin-1 bytes that are not valid UTF-8: the copy must be the bytes, not a decoding of them.
        var input = Input("reject: Bærum");

        var (result, context) = await Build(ArchiveMode.OnIncident, BrowseBaseUrl)
            .Execute(input, FileContext(), ct: CancellationToken.None);

        Assert.IsType<StepResult<CloudEvent>.RoutedIncident>(result);
        const string key = "orders/2026/10/28/order_1.xml";
        Assert.Equal(input.ToArray(), Assert.Single(_store.Files, f => f.Key == key).Value);
        Assert.Equal(key, context.Metadata[ArchiveContextKeys.Ref]);

        var incident = RoutedIncident()!;
        Assert.Equal("rejected", incident.Description);
        Assert.Equal("kept", incident.Context!["reason"]);
        Assert.Equal(key, incident.Context[ArchiveContextKeys.IncidentDataRef]);
        Assert.Equal(BrowseBaseUrl.AbsoluteUri + key, incident.Context[ArchiveContextKeys.IncidentDataUrl]);
    }

    [Fact]
    public async Task OnIncident_APublishedItemIsNotArchived()
    {
        var (result, _) = await Build(ArchiveMode.OnIncident).Execute(Input("accept"), FileContext(),
            ct: CancellationToken.None);

        Assert.IsType<StepResult<CloudEvent>.Success>(result);
        Assert.Equal(1, _published);
        Assert.Empty(_store.Files);
    }

    [Fact]
    public async Task OnIncident_WithoutABrowseBaseUrl_TheIncidentNamesOnlyTheKey()
    {
        await Build(ArchiveMode.OnIncident).Execute(Input("reject"), FileContext(), ct: CancellationToken.None);

        var incident = RoutedIncident()!;
        Assert.True(incident.Context!.ContainsKey(ArchiveContextKeys.IncidentDataRef));
        Assert.False(incident.Context.ContainsKey(ArchiveContextKeys.IncidentDataUrl));
    }

    // The write is required: an incident's item is let go of right after, so without a copy no
    // incident is raised and the item stays to be tried again.
    [Fact]
    public async Task OnIncident_AFailedWrite_IsATechnicalFailure_AndNoIncidentIsRaised()
    {
        _store.WriteException = new IOException("archive unavailable");

        var (result, _) = await Build(ArchiveMode.OnIncident).Execute(Input("reject"), FileContext(),
            ct: CancellationToken.None);

        Assert.IsType<StepResult<CloudEvent>.TechnicalFailure>(result);
        Assert.Null(RoutedIncident());
    }

    [Fact]
    public async Task OnReceipt_APublishedItemIsArchivedOnReceipt()
    {
        var input = Input("accept");

        var (result, context) = await Build(ArchiveMode.OnReceipt).Execute(input, FileContext(),
            ct: CancellationToken.None);

        Assert.IsType<StepResult<CloudEvent>.Success>(result);
        Assert.Equal(1, _published);
        Assert.Equal(input.ToArray(), Assert.Single(_store.Files).Value);
        Assert.Equal(_store.Files.Single().Key, context.Metadata[ArchiveContextKeys.Ref]);
    }

    // The copy written on receipt is the one the incident names: one payload, one write.
    [Fact]
    public async Task OnReceipt_AnIncidentReusesTheCopyWrittenOnReceipt()
    {
        await Build(ArchiveMode.OnReceipt).Execute(Input("reject"), FileContext(), ct: CancellationToken.None);

        Assert.Equal(1, _store.Writes);
        Assert.Equal(_store.Files.Single().Key, RoutedIncident()!.Context![ArchiveContextKeys.IncidentDataRef]);
    }

    // Archiving on receipt is best effort: an archive outage must not stop the integration.
    [Fact]
    public async Task OnReceipt_AFailedWriteOnReceipt_StillPublishesTheItem()
    {
        _store.WriteException = new IOException("archive unavailable");
        var context = FileContext();

        var (result, _) = await Build(ArchiveMode.OnReceipt).Execute(Input("accept"), context, ct: CancellationToken.None);

        Assert.IsType<StepResult<CloudEvent>.Success>(result);
        Assert.Equal(1, _published);
        Assert.False(context.Metadata.ContainsKey(ArchiveContextKeys.Ref));
    }

    // If the copy on receipt failed, the incident writes one, and that write is required.
    [Fact]
    public async Task OnReceipt_AnIncidentAfterAFailedWriteOnReceipt_WritesTheCopyItself()
    {
        _store.FailNextWrites = 1;

        await Build(ArchiveMode.OnReceipt).Execute(Input("reject"), FileContext(), ct: CancellationToken.None);

        Assert.Equal(2, _store.Writes);
        Assert.Equal(_store.Files.Single().Key, RoutedIncident()!.Context![ArchiveContextKeys.IncidentDataRef]);
    }

    // Without an archive the pipeline is exactly what it was before the feature existed.
    [Fact]
    public async Task WithoutAnArchive_NothingIsWritten_AndTheIncidentIsUnchanged()
    {
        await Build(null).Execute(Input("reject"), FileContext(), ct: CancellationToken.None);

        Assert.Empty(_store.Files);
        Assert.False(RoutedIncident()!.Context!.ContainsKey(ArchiveContextKeys.IncidentDataRef));
    }

    private sealed class TextDeserializer : DeserializeStep<string, Context>
    {
        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
            ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>(
                (new BusinessStepResult<string>.Success(Encoding.Latin1.GetString(input.Span)), context));
    }

    // Rejects every text that starts with "reject", as a terminal incident with context of its own.
    private sealed class RejectingValidator : ValidateStep<string, Context>
    {
        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
            string input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>(input.StartsWith("reject", StringComparison.Ordinal)
                ? (new BusinessStepResult<string>.Failure(
                    new BusinessIncidentData { Description = "rejected", Context = new() { ["reason"] = "kept" } },
                    Retriable: false), context)
                : (new BusinessStepResult<string>.Success(input), context));
    }

    private sealed class IdentityTransformer : TransformStep<string, string, Context>
    {
        public override Task<(TechnicalStepResult<string> Result, Context Context)> ExecuteAsync(
            string input, Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<string>, Context)>(
                (new TechnicalStepResult<string>.Success(input), context));
    }

    /// <summary>The archive store, recording what was written under which key.</summary>
    private sealed class RecordingStore : IFileAdapter
    {
        public Dictionary<string, byte[]> Files { get; } = new(StringComparer.Ordinal);

        public int Writes { get; private set; }

        public Exception? WriteException { get; set; }

        public int FailNextWrites { get; set; }

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null,
            CancellationToken ct = default)
        {
            Writes++;
            if (WriteException is not null)
                throw WriteException;
            if (FailNextWrites > 0)
            {
                FailNextWrites--;
                throw new IOException("archive unavailable");
            }

            Files[fileName] = content;
            return Task.CompletedTask;
        }

        public Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default) =>
            Task.FromResult(Files[fileName]);

        public Task<List<FileEntry>> ListAsync(CancellationToken ct = default) =>
            Task.FromResult(new List<FileEntry>());

        public Task DeleteAsync(string fileName, CancellationToken ct = default) => Task.CompletedTask;
    }
}
