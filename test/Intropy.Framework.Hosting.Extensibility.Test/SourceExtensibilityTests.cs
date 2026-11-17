using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Acme.Adapters.Inbox;
using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.Sweeps;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Hosting.Extensibility.Test;

/// <summary>
/// The source extension contract, used from outside Hosting: a kind of source Hosting does not
/// ship (<see cref="InboxSourcePort"/>) plugs into a component next to a file port, and a custom
/// job reuses a framework source with a handler of its own.
/// </summary>
public class SourceExtensibilityTests
{
    private readonly IIdempotencyServiceClient _idempotency = Substitute.For<IIdempotencyServiceClient>();
    private readonly RecordingSender _sender = new();
    private readonly InMemoryFileAdapter _orders = new();
    private readonly Inbox _inbox = new();

    public SourceExtensibilityTests() =>
        _idempotency.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));

    [Fact]
    public void Hosting_GrantsThisAssemblyNoAccessToItsInternals()
    {
        // The guard that makes the rest meaningful: what compiles here compiles in any adapter package.
        var friends = typeof(SourcePort).Assembly.GetCustomAttributes<InternalsVisibleToAttribute>()
            .Select(a => a.AssemblyName.Split(',')[0]);

        Assert.DoesNotContain(typeof(SourceExtensibilityTests).Assembly.GetName().Name, friends);
    }

    [Fact]
    public async Task AnExtractor_SweepsAnExternalKindOfSourceNextToAFilePort()
    {
        _orders.AddFile("order.csv", "o-1");
        _inbox.Add("msg-1", "i-1", batch: "b-42");
        await using var provider = Extractor(sources => sources.Port("orders").Port(new InboxSourcePort("inbox", _inbox)));

        var summary = await RunAsync(provider);

        Assert.Equal(new RunSummary(2, 0, 0), summary);
        Assert.Equal(["orders", "inbox"], _sender.Sent.Select(e => e[CloudEventExtensionKeys.SourcePort]));
        Assert.Equal("i-1", _sender.Sent[1].Data);
        // The inbox's own metadata reached the context: the route keys its idempotency on it.
        await _idempotency.Received(1).GetStatusAsync(Arg.Is<MessageInfo>(m => m.Id == "inbox:b-42"));
        Assert.Empty(_inbox.Pending);
        Assert.Empty(_orders.Files);
    }

    [Fact]
    public async Task AnExtractor_LeavesAnUnhandledItemOfAnExternalKindInItsSource()
    {
        _inbox.Add("msg-1", "i-1", batch: "b-42");
        _sender.Fail = true;
        await using var provider = Extractor(sources => sources.Port("orders").Port(new InboxSourcePort("inbox", _inbox)));

        var summary = await RunAsync(provider);

        Assert.Equal(new RunSummary(0, 1, 0), summary);
        Assert.Equal("msg-1", Assert.Single(_inbox.Pending).Id);
    }

    [Fact]
    public async Task AnExtractor_FailsAndKeepsAnItemOfAnExternalKindWithNoContent()
    {
        // The extractor's rule for an empty item holds whatever kind of source it came from.
        _inbox.Add("empty", "", batch: "b-42").Add("msg-2", "i-2", batch: "b-43");
        await using var provider = Extractor(sources => sources.Port("orders").Port(new InboxSourcePort("inbox", _inbox)));

        var summary = await RunAsync(provider);

        Assert.Equal(new RunSummary(1, 1, 0), summary);
        Assert.Equal("empty", Assert.Single(_inbox.Pending).Id);
        Assert.Equal("i-2", Assert.Single(_sender.Sent).Data);
    }

    [Fact]
    public async Task AnExternalKindOfSource_IsAComponentsSingleSourcePort()
    {
        _inbox.Add("msg-1", "i-1", batch: "b-42");
        var services = Services();
        services.AddSourcePort(new InboxSourcePort("inbox", _inbox));
        services.AddExtractor<string, string>(Pipeline);
        await using var provider = services.BuildServiceProvider();

        var summary = await RunAsync(provider);

        Assert.Equal(new RunSummary(1, 0, 0), summary);
        Assert.Empty(_inbox.Pending);
    }

    [Fact]
    public async Task AnExternalKindOfSource_IsADefinitionsSource()
    {
        _inbox.Add("msg-1", "i-1", batch: "b-42");
        var services = Services();
        services.AddExtractor(new ExtractorDefinition<string, string, Context>
        {
            Source = new InboxSourcePort("inbox", _inbox),
            Pipeline = Pipeline,
        });
        await using var provider = services.BuildServiceProvider();

        var summary = await RunAsync(provider);

        Assert.Equal(new RunSummary(1, 0, 0), summary);
        Assert.Empty(_inbox.Pending);
    }

    [Fact]
    public async Task ACustomJob_SweepsAFrameworkSourceWithAHandlerOfItsOwn()
    {
        // Reuse what fits (listing files, completing them), replace what doesn't (the handling).
        _orders.AddFile("a.csv", "keep").AddFile("b.csv", "take");
        var services = new ServiceCollection().AddKeyedSingleton<IFileAdapter>("orders", _orders);
        await using var provider = services.BuildServiceProvider();
        var sweep = new FileSourcePort("orders", FileCompletion.Delete)
            .CreateSweep(new SourceSweepContext(provider, "custom-job", NullLoggerFactory.Instance));
        var handled = new List<(string Id, string Content, string FileName)>();

        await sweep.ValidateAsync(CancellationToken.None);
        var summary = await sweep.SweepAsync(async (item, ct) =>
        {
            var content = await item.ReadTextAsync(Encoding.UTF8, ct);
            handled.Add((item.Id, content, item.Metadata[SourceContextKeys.FileName]));
            return content == "take" ? SweepOutcome.Consumed : SweepOutcome.Failed;
        }, CancellationToken.None);

        Assert.Equal(new RunSummary(1, 1, 0), summary);
        Assert.Equal([("a.csv", "keep", "a.csv"), ("b.csv", "take", "b.csv")], handled.OrderBy(h => h.Id));
        Assert.Equal(["a.csv"], _orders.Files.Keys);
    }

    [Fact]
    public async Task SweepItemRunner_CompletesOnlyHandledItems_AndCountsTheRun()
    {
        await using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new SweepItemRunner(new SourceSweepContext(provider, "custom-job", NullLoggerFactory.Instance), "inbox");
        _inbox.Add("ok", "1", "b");
        _inbox.Add("dup", "2", "b");
        _inbox.Add("bad", "3", "b");
        _inbox.Add("throws", "4", "b");
        var outcomes = new Dictionary<string, Func<SweepOutcome>>
        {
            ["ok"] = () => SweepOutcome.Consumed,
            ["dup"] = () => SweepOutcome.Duplicate,
            ["bad"] = () => SweepOutcome.Failed,
            ["throws"] = () => throw new InvalidOperationException("boom"),
        };
        var completed = new List<(string, SweepOutcome)>();

        foreach (var message in _inbox.List())
            await runner.RunAsync(message.Id, scope => new TextItem(message.Id, "inbox", scope),
                (item, _) => Task.FromResult(outcomes[item.Id]()),
                (item, outcome) =>
                {
                    completed.Add((item.Id, outcome));
                    return Task.CompletedTask;
                },
                CancellationToken.None);

        Assert.Equal([("ok", SweepOutcome.Consumed), ("dup", SweepOutcome.Duplicate)], completed);
        Assert.Equal(new RunSummary(1, 2, 1), runner.Finish());
    }

    [Fact]
    public async Task SweepItemRunner_KeepsAnItemWhoseCompletionFails()
    {
        await using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new SweepItemRunner(new SourceSweepContext(provider, "custom-job", NullLoggerFactory.Instance), "inbox");

        var outcome = await runner.RunAsync("msg-1", scope => new TextItem("msg-1", "inbox", scope),
            (_, _) => Task.FromResult(SweepOutcome.Consumed),
            (_, _) => throw new IOException("ack failed"),
            CancellationToken.None);

        Assert.Equal(SweepOutcome.Failed, outcome);
        Assert.Equal(new RunSummary(0, 1, 0), runner.Finish());
    }

    private ServiceProvider Extractor(Action<Sources> sources)
    {
        var services = Services();
        services.AddKeyedSingleton<IFileAdapter>("orders", _orders);
        services.AddExtractor(sources, routes => routes
            .On<string, string>(["orders"], Pipeline)
            .On<string, string>(["inbox"], (builder, sp) => Pipeline(builder, sp)
                .WithIdempotency((_, context) => context.Metadata[InboxSourcePort.BatchKey], (_, _) => DateTimeOffset.UnixEpoch)));
        return services.BuildServiceProvider();
    }

    private static ExtractorBuilder<string, string, Context> Pipeline(ExtractorBuilder<string, string, Context> builder,
        IServiceProvider _) => builder
        .WithDeserializer(new TextDeserializer())
        .WithValidator(new AcceptValidator())
        .WithTransformer(new IdentityTransformer())
        .WithSerializer(new CloudEventSerializeStep<string, Context>(text => text, _ => DateTimeOffset.UnixEpoch,
            type: (_, _) => "text.received"))
        .WithSenderFromServices()
        .WithBusinessIncidents(_ => "unknown", _ => "unknown");

    private ServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "acme-extractor";
            options.ServiceNamespace = "example";
        });
        services.AddSingleton(_idempotency);
        services.AddSingleton(Substitute.For<IBusinessIncidentServiceClient>());
        services.AddSingleton<SendStep<Context>>(_sender);
        return services;
    }

    private static Task<RunSummary> RunAsync(IServiceProvider provider) =>
        provider.GetRequiredService<IJob>().ExecuteAsync(CancellationToken.None);

    private sealed class TextItem(string id, string port, IServiceProvider services) : SweptItem(id, port, services)
    {
        public override Task<byte[]> ReadAsync(CancellationToken ct = default) =>
            Task.FromResult(Encoding.UTF8.GetBytes(Id));
    }

    private sealed class TextDeserializer : DeserializeStep<string, Context>
    {
        public override string StepName => "Deserialize";

        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
            ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>(
                (new BusinessStepResult<string>.Success(Encoding.UTF8.GetString(input.Span)), context));
    }

    private sealed class AcceptValidator : ValidateStep<string, Context>
    {
        public override string StepName => "Validate";

        public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(string input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<string>, Context)>((new BusinessStepResult<string>.Success(input), context));
    }

    private sealed class IdentityTransformer : TransformStep<string, string, Context>
    {
        public override string StepName => "Transform";

        public override Task<(TechnicalStepResult<string> Result, Context Context)> ExecuteAsync(string input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<string>, Context)>((new TechnicalStepResult<string>.Success(input), context));
    }

    private sealed class RecordingSender : SendStep<Context>
    {
        public List<CloudEvent> Sent { get; } = [];

        public bool Fail { get; set; }

        public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(CloudEvent input,
            Context context, CancellationToken ct)
        {
            if (Fail)
                return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
                    (new TechnicalStepResult<CloudEvent>.Failure(new TechnicalFailure("The target refused it")), context));

            Sent.Add(input);
            return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
                (new TechnicalStepResult<CloudEvent>.Success(input), context));
        }
    }
}
