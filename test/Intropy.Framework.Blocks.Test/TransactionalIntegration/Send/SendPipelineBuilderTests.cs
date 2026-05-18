using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Send;

public class SendPipelineBuilderTests
{
    private readonly ILoggerFactory _loggerFactory = Substitute.For<ILoggerFactory>();

    private readonly IBusinessIncidentServiceClient _businessIncidentServiceClient =
        Substitute.For<IBusinessIncidentServiceClient>();

    private readonly IIdempotencyServiceClient _idempotencyServiceClient = Substitute.For<IIdempotencyServiceClient>();

    private readonly FrameworkOptions _frameworkOptions = new() { ComponentName = "Test",  ServiceNamespace = "Org" };

    private readonly DeserializeStep<int, Context> _deserializer = Substitute.For<DeserializeStep<int, Context>>();
    private readonly ExtractStep<int, Context> _extractor = Substitute.For<ExtractStep<int, Context>>();
    private readonly ExtractStep<int, Context> _extractor2 = Substitute.For<ExtractStep<int, Context>>();
    private readonly ValidateStep<int, Context> _validator = Substitute.For<ValidateStep<int, Context>>();
    private readonly TransformStep<int, int, Context> _transformer = Substitute.For<TransformStep<int, int, Context>>();
    private readonly SerializeStep<int, Context> _serializer = Substitute.For<SerializeStep<int, Context>>();
    private readonly SendStep<Context> _sender = Substitute.For<SendStep<Context>>();

    [Fact]
    public void Create_WithNullPipelineName_ThrowsException()
    {
        // Arrange
        string pipelineName = null!;

        // Act & assert
        Assert.Throws<ArgumentNullException>(() =>
            SendPipelineBuilder<int, int, Context>.Create(pipelineName, _frameworkOptions,
                _loggerFactory));
    }

    [Fact]
    public void Create_WithEmptyPipelineName_ThrowsException()
    {
        // Arrange
        const string pipelineName = "";

        // Act & assert
        Assert.Throws<ArgumentException>(() =>
            SendPipelineBuilder<int, int, Context>.Create(pipelineName, _frameworkOptions,
                _loggerFactory));
    }

    [Fact]
    public void Create_WithNullILoggerFactory_ThrowsException()
    {
        // Arrange
        const string pipelineName = "Test";

        // Act & assert
        Assert.Throws<ArgumentNullException>(() =>
            SendPipelineBuilder<int, int, Context>.Create(pipelineName, _frameworkOptions,
                null!));
    }

    [Fact]
    public void Create_WithNullFrameworkOptions_ThrowsException()
    {
        // Arrange
        const string pipelineName = "Test";

        // Act & assert
        Assert.Throws<ArgumentNullException>(() =>
            SendPipelineBuilder<int, int, Context>.Create(pipelineName, null!,
                _loggerFactory));
    }

    [Fact]
    public void Create_WithValidArguments_CreatesTransactionalIntegration()
    {
        // Arrange
        const string pipelineName = "Test";

        // Act
        var result =
            SendPipelineBuilder<int, int, Context>.Create(pipelineName, _frameworkOptions,
                _loggerFactory);

        // Assert
        Assert.IsType<SendPipelineBuilder<int, int, Context>>(result);
    }

    [Theory]
    [InlineData(typeof(ValidateStep<,>))]
    [InlineData(typeof(DeserializeStep<,>))]
    [InlineData(typeof(IdempotencyCheckStep<,>))]
    [InlineData(typeof(TransformStep<,,>))]
    [InlineData(typeof(SerializeStep<,>))]
    [InlineData(typeof(SendStep<>))]
    [InlineData(typeof(BusinessIncidentRouteStep<,>))]
    public void Build_WhenDependencyMissing_ThrowsInvalidOperationException(Type missingDependency)
    {
        // Arrange
        var builder = ConfigureAllDependenciesExcept(missingDependency);
        var exceptionMessageMatch = missingDependency.Name.Split('`').First();

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains(exceptionMessageMatch, exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_WithAllDependencies_CreatesTransactionalIntegration()
    {
        // Arrange
        var builder = GetBuilder()
            .WithDeserializer(_deserializer)
            .WithIdempotency(_idempotencyServiceClient, (_, _) => "test", (_, _) => DateTime.Now)
            .WithExtractor(_extractor)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSerializer(_serializer)
            .WithSender(_sender)
            .WithBusinessIncidents(_businessIncidentServiceClient, x => x.Metadata["TEST"],
                x => x.Metadata["TEST"]);

        // Act
        var result = builder.Build();

        // Assert
        Assert.IsType<SendPipeline<int, int, Context>>(result);
    }

    [Fact]
    public void Build_WithTwoExtractors_Succeeds()
    {
        // Arrange
        var builder = GetBuilder()
            .WithDeserializer(_deserializer)
            .WithExtractor(_extractor)
            .WithExtractor(_extractor2)
            .WithIdempotency(_idempotencyServiceClient, (_, _) => "test", (_, _) => DateTime.Now)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSerializer(_serializer)
            .WithSender(_sender)
            .WithBusinessIncidents(_businessIncidentServiceClient, x => x.Metadata["TEST"],
                x => x.Metadata["TEST"]);
        
        // Act
        var result = builder.Build();
        
        // Assert
        Assert.IsType<SendPipeline<int, int, Context>>(result);
    }

    private SendPipelineBuilder<int, int, Context> ConfigureAllDependenciesExcept(Type skip)
    {
        var builder = GetBuilder();

        if (skip != typeof(DeserializeStep<,>))
            builder.WithDeserializer(_deserializer);

        if (skip != typeof(IdempotencyCheckStep<,>))
            builder.WithIdempotency(_idempotencyServiceClient, (_, _) => "test", (_, _) => DateTime.Now);

        if (skip != typeof(ExtractStep<,>))
            builder.WithExtractor(_extractor);

        if (skip != typeof(ValidateStep<,>))
            builder.WithValidator(_validator);

        if (skip != typeof(TransformStep<,,>))
            builder.WithTransformer(_transformer);

        if (skip != typeof(SerializeStep<,>))
            builder.WithSerializer(_serializer);

        if (skip != typeof(SendStep<>))
            builder.WithSender(_sender);

        if (skip != typeof(BusinessIncidentRouteStep<,>))
            builder.WithBusinessIncidents(_businessIncidentServiceClient, x => x.Metadata["TEST"],
                x => x.Metadata["TEST"]);

        return builder;
    }

    private SendPipelineBuilder<int, int, Context> GetBuilder()
    {
        return SendPipelineBuilder<int, int, Context>.Create("Test", _frameworkOptions,
            _loggerFactory);
    }
}
