using System.Text;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
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
        Assert.IsType<StepResult<string>.Success>(result);
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

            Assert.IsType<StepResult<string>.Success>(result);
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
        Assert.IsType<StepResult<string>.Success>(result);
        await _businessIncidentServiceClient.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
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
