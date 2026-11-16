using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Adapters.Common;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using LoaderSteps = Intropy.Framework.Blocks.Loader.Steps;
using SendSteps = Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;

namespace Intropy.Framework.Blocks.Test.Shared;

/// <summary>
/// The loader and the send side of a Transactional Integration archive the same way the extractor
/// does (see ExtractorArchiveTests); what differs is the item a copy is named after — the event's id,
/// and the queued message's id rather than the file name it was received as.
/// </summary>
public class ArchivePipelinesTests
{
    private static readonly byte[] Payload = "reject"u8.ToArray();

    private readonly RecordingStore _store = new();
    private readonly IBusinessIncidentServiceClient _incidents = Substitute.For<IBusinessIncidentServiceClient>();

    private ArchivePolicy Policy() => new("orders", _store, ArchiveMode.OnIncident, null, TimeProvider.System);

    [Fact]
    public async Task ALoader_ArchivesAnIncidentsPayload_UnderTheEventsIdAndTime()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_incidents);
        services.AddIntropyFramework(o =>
        {
            o.ComponentName = "orders";
            o.ServiceNamespace = "example";
        });
        var loader = LoaderBuilder<string, string, Context>.Create("orders.Process", services.BuildServiceProvider())
            .WithDeserializer(new LoaderDeserializer())
            .WithValidator(new LoaderRejectingValidator())
            .WithTransformer(new LoaderIdentityTransformer())
            .WithSender(new LoaderDiscardingSender())
            .WithBusinessIncidents(_ => "orders", _ => "orders")
            .WithArchive(Policy())
            .Build();
        var context = new Context(new Dictionary<string, string>
        {
            [CloudEventContextKeys.Id] = "evt-1",
            [CloudEventContextKeys.Time] = "2026-10-27T08:00:00Z"
        });

        await loader.Execute(Payload, context, detachTrace: false, CancellationToken.None);

        Assert.Equal(Payload, Assert.Single(_store.Files, f => f.Key == "orders/2026/10/27/evt-1").Value);
    }

    // A queued message carries the file name it was received as too; two messages can come from
    // files of the same name, so the message id names the copy.
    [Fact]
    public async Task ASendPipeline_ArchivesAnIncidentsPayload_UnderTheMessagesId()
    {
        var pipeline = SendPipelineBuilder<string, string, Context>
            .Create("orders.Send", new FrameworkOptions { ComponentName = "orders", ServiceNamespace = "example" },
                NullLoggerFactory.Instance)
            .WithDeserializer(new SendDeserializer())
            .WithValidator(new SendRejectingValidator())
            .WithTransformer(new SendIdentityTransformer())
            .WithSerializer(new SendSerializer())
            .WithSender(new SendDiscardingSender())
            .WithBusinessIncidents(_incidents, _ => "orders", _ => "orders")
            .WithArchive(Policy())
            .Build();
        var context = new Context(new Dictionary<string, string>
        {
            [ContextKeys.MessageId] = "msg-1",
            [SourceContextKeys.FileName] = "order.xml",
            [SourceContextKeys.ReceivedAt] = "2026-10-26T08:00:00.0000000Z"
        });

        await pipeline.Execute(Payload, context, CancellationToken.None);

        Assert.Equal(Payload, Assert.Single(_store.Files, f => f.Key == "orders/2026/10/26/msg-1").Value);
    }

    private static BusinessIncidentData Rejected => new() { Description = "rejected" };

    private sealed class LoaderDeserializer : LoaderSteps.DeserializeStep<string, Context>
    {
        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
            ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>(
                (new BusinessStepResult<string>.Success(System.Text.Encoding.UTF8.GetString(input.Span)), context));
    }

    private sealed class LoaderRejectingValidator : LoaderSteps.ValidateStep<string, Context>
    {
        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
            string input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>(
                (new BusinessStepResult<string>.Failure(Rejected, Retriable: false), context));
    }

    private sealed class LoaderIdentityTransformer : LoaderSteps.TransformStep<string, string, Context>
    {
        public override Task<(TechnicalStepResult<string> Result, Context Context)> ExecuteAsync(
            string input, Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<string>, Context)>(
                (new TechnicalStepResult<string>.Success(input), context));
    }

    private sealed class LoaderDiscardingSender : LoaderSteps.SendStep<string, Context>
    {
        public override Task<(TechnicalStepResult<string> Result, Context Context)> ExecuteAsync(
            string input, Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<string>, Context)>(
                (new TechnicalStepResult<string>.Success(input), context));
    }

    private sealed class SendDeserializer : SendSteps.DeserializeStep<string, Context>
    {
        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
            ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>(
                (new BusinessStepResult<string>.Success(System.Text.Encoding.UTF8.GetString(input.Span)), context));
    }

    private sealed class SendRejectingValidator : SendSteps.ValidateStep<string, Context>
    {
        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
            string input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>(
                (new BusinessStepResult<string>.Failure(Rejected, Retriable: false), context));
    }

    private sealed class SendIdentityTransformer : SendSteps.TransformStep<string, string, Context>
    {
        public override Task<(TechnicalStepResult<string> Result, Context Context)> ExecuteAsync(
            string input, Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<string>, Context)>(
                (new TechnicalStepResult<string>.Success(input), context));
    }

    private sealed class SendSerializer : SendSteps.SerializeStep<string, Context>
    {
        public override Task<(TechnicalStepResult<byte[]> Result, Context Context)> ExecuteAsync(
            string input, Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<byte[]>, Context)>(
                (new TechnicalStepResult<byte[]>.Success(System.Text.Encoding.UTF8.GetBytes(input)), context));
    }

    private sealed class SendDiscardingSender : SendSteps.SendStep<Context>
    {
        public override Task<(BusinessStepResult<byte[]> Result, Context Context)> ExecuteAsync(
            byte[] input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<byte[]>, Context)>(
                (new BusinessStepResult<byte[]>.Success(input), context));
    }

    private sealed class RecordingStore : IFileAdapter
    {
        public Dictionary<string, byte[]> Files { get; } = new(StringComparer.Ordinal);

        public Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null,
            CancellationToken ct = default)
        {
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
