using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Test.Extractor;

/// <summary>
/// An extractor pipeline built for one source port: it stamps the port on the CloudEvent it sends
/// and keys its idempotency records on the port by default, sharing them across ports when the
/// entity scope is chosen. A pipeline built without a port does neither.
/// </summary>
public class ExtractorSourcePortTests
{
    private const string Port = "orders-se";
    private readonly IIdempotencyServiceClient _idempotency = Substitute.For<IIdempotencyServiceClient>();
    private readonly RecordingSender _sender = new();
    private readonly IServiceProvider _provider;

    public ExtractorSourcePortTests()
    {
        _idempotency.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));

        var services = new ServiceCollection();
        services.AddSingleton(_idempotency);
        services.AddSingleton(Substitute.For<IBusinessIncidentServiceClient>());
        services.AddSingleton(Substitute.For<ILoggerFactory>());
        services.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "ExtractorTest";
            conf.ServiceNamespace = "Org";
        });
        _provider = services.BuildServiceProvider();
    }

    [Fact]
    public async Task Execute_WithoutPort_KeysOnTheIdAndStampsNoPort()
    {
        await Execute(CreateBuilder(port: null).WithIdempotency(Id, Date));

        await AssertIdAsync("1337");
        Assert.Null(_sender.Sent?[CloudEventExtensionKeys.SourcePort]);
    }

    [Fact]
    public async Task Execute_ForPortWithDefaultScope_KeysOnPortAndId()
    {
        await Execute(CreateBuilder(Port).WithIdempotency(Id, Date));

        await AssertIdAsync($"{Port}:1337");
    }

    [Fact]
    public async Task Execute_ForPortWithEntityScope_KeysOnTheId()
    {
        await Execute(CreateBuilder(Port).WithIdempotency(Id, Date, scope: IdempotencyScope.Entity));

        await AssertIdAsync("1337");
    }

    [Fact]
    public async Task Execute_ForPort_StampsThePortOnTheSentEvent()
    {
        var (result, _) = await Execute(CreateBuilder(Port).WithIdempotency(Id, Date));

        Assert.IsType<StepResult<CloudEvent>.Success>(result);
        Assert.Equal(Port, _sender.Sent?[CloudEventExtensionKeys.SourcePort]);
    }

    [Fact]
    public void Create_RejectsAnEmptyPort() =>
        Assert.Throws<ArgumentException>(() =>
            ExtractorBuilder<CustomerIn, CustomerOut, Context>.Create("ExtractorTest", _provider, " "));

    private static string Id(CustomerIn input, Context _) => input.CustomerId;

    private static DateTimeOffset Date(CustomerIn input, Context _) => input.EventDate;

    private ExtractorBuilder<CustomerIn, CustomerOut, Context> CreateBuilder(string? port)
    {
        var builder = port is null
            ? ExtractorBuilder<CustomerIn, CustomerOut, Context>.Create("ExtractorTest", _provider)
            : ExtractorBuilder<CustomerIn, CustomerOut, Context>.Create("ExtractorTest", _provider, port);
        return builder
            .WithDeserializer(new CustomerDeserializer())
            .WithValidator(new CustomerValidator())
            .WithTransformer(new CustomerTransformer())
            .WithSerializer(new CustomerSerializer())
            .WithSender(_sender)
            .WithBusinessIncidents(_ => "unknown", _ => "unknown");
    }

    private static Task<(StepResult<CloudEvent> Result, Context Context)> Execute(
        ExtractorBuilder<CustomerIn, CustomerOut, Context> builder) =>
        builder.Build().Execute("1337"u8.ToArray(), new Context(new Dictionary<string, string>()));

    private async Task AssertIdAsync(string expectedId)
    {
        await _idempotency.Received(1).GetStatusAsync(Arg.Is<MessageInfo>(m => m.Id == expectedId));
        await _idempotency.Received(1).CommitAsync(Arg.Is<MessageInfo>(m => m.Id == expectedId));
    }

    private sealed class RecordingSender : SendStep<Context>
    {
        public CloudEvent? Sent { get; private set; }

        public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(
            CloudEvent input, Context context, CancellationToken ct)
        {
            Sent = input;
            return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
                (new TechnicalStepResult<CloudEvent>.Success(input), context));
        }
    }
}
