using System.Text;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Send;

public class SendPipelineTests
{
    private readonly IIdempotencyServiceClient _idempotencyServiceClient;
    private readonly IBusinessIncidentServiceClient _businessIncidentServiceClient;
    private readonly FrameworkOptions _frameworkOptions = new() { ComponentName = "Test", ServiceNamespace = "Org" };
    private readonly ILoggerFactory _loggerFactory = Substitute.For<ILoggerFactory>();

    public SendPipelineTests()
    {
        _idempotencyServiceClient = Substitute.For<IIdempotencyServiceClient>();
        _businessIncidentServiceClient = Substitute.For<IBusinessIncidentServiceClient>();

        _idempotencyServiceClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));

        _businessIncidentServiceClient.Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
                Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>())
            .ReturnsForAnyArgs(Task.CompletedTask);

        var mockLogger = Substitute.For<ILogger>();
        _loggerFactory.CreateLogger(Arg.Any<string>()).Returns(mockLogger);
    }

    [Fact]
    public async Task Pipeline_ShouldProcess()
    {
        // Arrange
        var builder = GetPreparedPipelineBuilder();
        var pipeline = builder.Build();
        const string rawInput = """
                                {"orderId": "123", "eventDateTime": "2025-01-01T00:00:01"}
                                """;
        var input = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(rawInput));
        var context = MyContext.Create();

        // Act
        var (result, _) = await pipeline.Execute(input, context);

        // Assert
        Assert.IsType<StepResult<byte[]>.Success>(result);
    }

    [Fact]
    public async Task Pipeline_ShouldProcessMultiple()
    {
        // Arrange
        var builder = GetPreparedPipelineBuilder();
        var pipeline = builder.Build();
        var rawInputs = new[]
        {
            "{\"orderId\": \"123\", \"eventDateTime\": \"2025-01-01T00:00:01\"}",
            "{\"orderId\": \"124\", \"eventDateTime\": \"2025-01-02T00:00:01\"}",
            "{\"orderId\": \"125\", \"eventDateTime\": \"2025-01-03T00:00:01\"}"
        };


        // Act & Assert
        foreach (var rawInput in rawInputs)
        {
            var input = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(rawInput));
            var context = MyContext.Create();
            var (result, _) = await pipeline.Execute(input, context);

            Assert.IsType<StepResult<byte[]>.Success>(result);
        }

        await _idempotencyServiceClient.ReceivedWithAnyArgs(rawInputs.Length).GetStatusAsync(Arg.Any<MessageInfo>());
        await _idempotencyServiceClient.ReceivedWithAnyArgs(rawInputs.Length).CommitAsync(Arg.Any<MessageInfo>());
    }

    [Fact]
    public async Task Pipeline_ShouldRouteBusinessIncident_WhenValidationFails()
    {
        // Arrange
        var builder = GetPreparedPipelineBuilder();
        var pipeline = builder.Build();
        const string rawInput = """
                                {"orderId": "_", "eventDateTime": "2025-01-01T00:00:01"}
                                """;
        var input = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(rawInput));
        var context = MyContext.Create();
        context.Metadata.Add("MessageId", "_");

        // Act
        var (result, _) = await pipeline.Execute(input, context);

        // Assert
        Assert.IsType<StepResult<byte[]>.RoutedIncident>(result);
        await _businessIncidentServiceClient.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Pipeline_ShouldProcess_WhenIdempotencyNotConfigured()
    {
        // Arrange
        var pipeline = SendPipelineBuilder<MyInput, MyOutput, MyContext>
            .Create("TestPipeline", _frameworkOptions, _loggerFactory)
            .WithDeserializer(new JsonDeserializer())
            .WithExtractor(new PassThroughExtractor())
            .WithValidator(new SchemaValidator())
            .WithTransformer(new MyBusinessTransformer())
            .WithSerializer(new XmlSerializer())
            .WithSender(new HttpSender())
            .WithBusinessIncidents(_businessIncidentServiceClient, ctx => ctx.Metadata["MessageId"],
                ctx => ctx.Metadata["MessageId"])
            .Build();

        const string rawInput = """
                                {"orderId": "123", "eventDateTime": "2025-01-01T00:00:01"}
                                """;
        var input = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(rawInput));
        var context = MyContext.Create();

        // Act
        var (result, _) = await pipeline.Execute(input, context);

        // Assert
        Assert.IsType<StepResult<byte[]>.Success>(result);
        await _idempotencyServiceClient.DidNotReceiveWithAnyArgs().GetStatusAsync(Arg.Any<MessageInfo>());
        await _idempotencyServiceClient.DidNotReceiveWithAnyArgs().CommitAsync(Arg.Any<MessageInfo>());
    }

    [Fact]
    public async Task Pipeline_ShouldProcess_WhenBusinessIncidentsNotConfigured()
    {
        // Arrange
        var pipeline = SendPipelineBuilder<MyInput, MyOutput, MyContext>
            .Create("TestPipeline", _frameworkOptions, _loggerFactory)
            .WithDeserializer(new JsonDeserializer())
            .WithIdempotency(_idempotencyServiceClient, (i, _) => i.OrderId, (i, _) => i.EventDateTime)
            .WithExtractor(new PassThroughExtractor())
            .WithValidator(new SchemaValidator())
            .WithTransformer(new MyBusinessTransformer())
            .WithSerializer(new XmlSerializer())
            .WithSender(new HttpSender())
            .Build();

        const string rawInput = """
                                {"orderId": "_", "eventDateTime": "2025-01-01T00:00:01"}
                                """;
        var input = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(rawInput));
        var context = MyContext.Create();

        // Act
        var (result, _) = await pipeline.Execute(input, context);

        // Assert - validation fails and there's no router to convert the failure to success
        Assert.IsType<StepResult<byte[]>.BusinessFailure>(result);
        await _businessIncidentServiceClient.DidNotReceiveWithAnyArgs().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Pipeline_SendsTheSerializedBytesUntouched()
    {
        // Arrange: ISO-8859-1 bytes are not valid UTF-8; the serializer produces them and the sender
        // must receive exactly those bytes (no string in between to corrupt æøå).
        var latin1 = Encoding.Latin1.GetBytes("<Order>blåbär</Order>");
        var sender = new CapturingSender();
        var pipeline = GetPreparedPipelineBuilder()
            .WithSerializer(new FixedBytesSerializer(latin1))
            .WithSender(sender)
            .Build();
        var input = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(
            """{"orderId": "123", "eventDateTime": "2025-01-01T00:00:01"}"""));

        // Act
        var (result, _) = await pipeline.Execute(input, MyContext.Create());

        // Assert
        Assert.IsType<StepResult<byte[]>.Success>(result);
        Assert.Equal(latin1, sender.Received);
    }

    [Fact]
    public async Task Pipeline_WhenTheDestinationCannotBeReached_RoutesAnIncidentAndKeepsTheFailure()
    {
        // The reported bug: an exception out of the adapter used to be acknowledged, consuming a
        // message whose payload was intact. It must now survive for redelivery — with the incident
        // still raised, because an unreachable destination is a business problem — and must not be
        // recorded as processed.
        var pipeline = GetPreparedPipelineBuilder().WithSender(new UnreachableDestinationSender()).Build();
        var context = MyContext.Create();
        context.Metadata.Add("MessageId", "ORD-1");

        var (result, _) = await pipeline.Execute(Input("123"), context);

        var failure = Assert.IsType<StepResult<byte[]>.BusinessFailure>(result);
        Assert.True(failure.Retriable);
        await _businessIncidentServiceClient.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        // Nothing was delivered, so nothing may be recorded: the redelivery has to run the pipeline.
        await _idempotencyServiceClient.DidNotReceiveWithAnyArgs().CommitAsync(Arg.Any<MessageInfo>());
    }

    [Fact]
    public async Task Pipeline_WhenTheDestinationRejectsTheData_RoutesAnIncidentAndSucceeds()
    {
        // The other half of the same boundary: the destination was reached and refused the content.
        // Retrying cannot change that, so the incident is the handling and the message is consumed.
        var pipeline = GetPreparedPipelineBuilder().WithSender(new RejectingSender()).Build();
        var context = MyContext.Create();
        context.Metadata.Add("MessageId", "ORD-1");

        var (result, _) = await pipeline.Execute(Input("123"), context);

        // A success, as a routed incident: the subtype carries the incident for a host that answers a caller.
        Assert.IsAssignableFrom<StepResult<byte[]>.Success>(result);
        await _businessIncidentServiceClient.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    private static ReadOnlyMemory<byte> Input(string orderId) => new(Encoding.UTF8.GetBytes(
        $"{{\"orderId\": \"{orderId}\", \"eventDateTime\": \"2025-01-01T00:00:01\"}}"));

    private sealed class FixedBytesSerializer(byte[] bytes) : SerializeStep<MyOutput, MyContext>
    {
        public override Task<(TechnicalStepResult<byte[]> Result, MyContext Context)> ExecuteAsync(MyOutput input,
            MyContext context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<byte[]>, MyContext)>(
                (new TechnicalStepResult<byte[]>.Success(bytes), context));
    }

    private sealed class CapturingSender : SendStep<MyContext>
    {
        public byte[]? Received { get; private set; }

        public override Task<(BusinessStepResult<byte[]> Result, MyContext Context)> ExecuteAsync(byte[] input,
            MyContext context, CancellationToken ct)
        {
            Received = input;
            return Task.FromResult<(BusinessStepResult<byte[]>, MyContext)>(
                (new BusinessStepResult<byte[]>.Success(input), context));
        }
    }

    private SendPipelineBuilder<MyInput, MyOutput, MyContext> GetPreparedPipelineBuilder()
    {
        var pipelineBuilder = SendPipelineBuilder<MyInput, MyOutput, MyContext>
            .Create("TestPipeline", _frameworkOptions, _loggerFactory)
            .WithDeserializer(new JsonDeserializer())
            .WithIdempotency(_idempotencyServiceClient, (input, _) => input.OrderId, (input, _) => input.EventDateTime)
            .WithExtractor(new PassThroughExtractor())
            .WithValidator(new SchemaValidator())
            .WithTransformer(new MyBusinessTransformer())
            .WithSerializer(new XmlSerializer())
            .WithSender(new HttpSender())
            .WithBusinessIncidents(_businessIncidentServiceClient, ctx => ctx.Metadata["MessageId"],
                ctx => ctx.Metadata["MessageId"]);

        return pipelineBuilder;
    }
}
