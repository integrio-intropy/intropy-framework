using Dapr.Client;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.Extractor;

public class ExtractorBuilderTests
{
    private readonly ExtractorBuilder<int, int, Context> _builder = GetBuilder();
    private readonly ExtractStep<int, Context> _extractor1 = Substitute.For<ExtractStep<int, Context>>();
    private readonly ExtractStep<int, Context> _extractor2 = Substitute.For<ExtractStep<int, Context>>();
    private readonly ExtractStep<int, Context> _extractor3 = Substitute.For<ExtractStep<int, Context>>();
    private readonly DeserializeStep<int, Context> _deserializer = Substitute.For<DeserializeStep<int, Context>>();
    private readonly ValidateStep<int, Context> _validator = Substitute.For<ValidateStep<int, Context>>();
    private readonly TransformStep<int, int, Context> _transformer = Substitute.For<TransformStep<int, int, Context>>();
    private readonly SerializeStep<int, Context> _serializer = Substitute.For<SerializeStep<int, Context>>();
    private readonly SendStep<Context> _sender = Substitute.For<SendStep<Context>>();

    [Fact]
    public void Create_WithNullServiceProvider_ThrowsException()
    {
        // Arrange
        IServiceProvider serviceProvider = null!;
        const string pipelineName = "Test";

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            ExtractorBuilder<int, int, Context>.Create(pipelineName, serviceProvider));
    }

    [Fact]
    public void Create_WithNullPipelineName_ThrowsException()
    {
        // Arrange
        IServiceProvider serviceProvider = new ServiceCollection().BuildServiceProvider();
        string pipelineName = null!;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            ExtractorBuilder<int, int, Context>.Create(pipelineName, serviceProvider));
    }

    [Fact]
    public void Create_WithEmptyPipelineName_ThrowsException()
    {
        // Arrange
        IServiceProvider serviceProvider = new ServiceCollection().BuildServiceProvider();
        const string pipelineName = "";

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            ExtractorBuilder<int, int, Context>.Create(pipelineName, serviceProvider));
    }

    [Fact]
    public void Create_WithNoRegisteredILoggerFactory_ThrowsException()
    {
        var serviceProvider = GetServiceCollection().RemoveAll<ILoggerFactory>().BuildServiceProvider();
        const string pipelineName = "Test";

        // Act & assert
        Assert.Throws<InvalidOperationException>(() =>
            ExtractorBuilder<int, int, Context>.Create(pipelineName, serviceProvider));
    }

    [Fact]
    public void Create_WithNoRegisteredFrameworkOptions_ThrowsException()
    {
        // Arrange
        var serviceProvider = GetServiceCollection().RemoveAll<FrameworkOptions>().BuildServiceProvider();
        const string pipelineName = "Test";

        // Act & assert
        Assert.Throws<InvalidOperationException>(() =>
            ExtractorBuilder<int, int, Context>.Create(pipelineName, serviceProvider));
    }

    [Fact]
    public void Build_ExtractStepsAreOptional()
    {
        // Arrange - all required steps, no extract steps                                                                                                              
        _builder
            .WithDeserializer(_deserializer)
            .WithIdempotency((_, _) => "test", (_, _) => DateTime.Now)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSerializer(_serializer)
            .WithSender(_sender)
            .WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST2"]
            );

        // Act & Assert - should not throw                                                                                                                             
        var result = _builder.Build();
        Assert.NotNull(result);
    }

    [Fact]
    public void Build_WithSingleExtractor_CreatesExtractor()
    {
        // Arrange                                                                                                                                                 
        _builder
            .WithDeserializer(_deserializer)
            .WithIdempotency((_, _) => "test", (_, _) => DateTime.Now)
            .WithExtractor(_extractor1)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSerializer(_serializer)
            .WithSender(_sender)
            .WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST2"]
            );

        // Act                                                                                                                                                     
        var result = _builder.Build();

        // Assert                                                                                                                                                  
        Assert.IsType<Extractor<int, int, Context>>(result);
    }

    [Fact]
    public void Build_WithMultipleExtractors_CreatesExtractor()
    {
        // Arrange                                                                                                                                                 
        _builder
            .WithDeserializer(_deserializer)
            .WithIdempotency((_, _) => "test", (_, _) => DateTime.Now)
            .WithExtractor(_extractor1)
            .WithExtractor(_extractor2)
            .WithExtractor(_extractor3)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSerializer(_serializer)
            .WithSender(_sender)
            .WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST2"]
            );

        // Act                                                                                                                                                     
        var result = _builder.Build();

        // Assert                                                                                                                                                  
        Assert.IsType<Extractor<int, int, Context>>(result);
    }

    [Theory]
    [InlineData("Deserialize")]
    [InlineData("Idempotency")]
    [InlineData("Validate")]
    [InlineData("Transform")]
    [InlineData("Serialize")]
    [InlineData("Sender")]
    [InlineData("Business incident route")]
    public void Build_WhenDependencyMissing_ThrowsInvalidOperationException(string missingDependency)
    {
        // Arrange
        ConfigureAllDependenciesExcept(missingDependency);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => _builder.Build());
        Assert.Contains(missingDependency, exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_WithAllDependencies_CreatesExtractor()
    {
        // Arrange
        _builder
            .WithDeserializer(_deserializer)
            .WithIdempotency((_, _) => "test", (_, _) => DateTime.Now)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSerializer(_serializer)
            .WithSender(_sender)
            .WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST2"]
            );

        // Act
        var result = _builder.Build();

        // Assert
        Assert.IsType<Extractor<int, int, Context>>(result);
    }

    [Fact]
    public void WithIdempotency_WhenIdempotencyServiceClientIsMissing_ThrowsException()
    {
        // Arrange
        var serviceProvider = GetServiceCollection().RemoveAll<IIdempotencyServiceClient>().BuildServiceProvider();
        var builder = ExtractorBuilder<int, int, Context>.Create("Test", serviceProvider);

        // Act & assert
        Assert.Throws<InvalidOperationException>(() =>
            builder.WithIdempotency((_, _) => "test", (_, _) => DateTime.Now));
    }

    private static ServiceCollection GetServiceCollection()
    {
        var idempotencyServiceClient = Substitute.For<IIdempotencyServiceClient>();
        var businessIncidentServiceClient = Substitute.For<IBusinessIncidentServiceClient>();
        var daprClient = Substitute.For<DaprClient>();

        var serviceCollection = new ServiceCollection();
        serviceCollection.AddSingleton(idempotencyServiceClient);
        serviceCollection.AddSingleton(businessIncidentServiceClient);
        serviceCollection.AddSingleton(daprClient);
        serviceCollection.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "Test";
            conf.ServiceNamespace = "Org";
        });

        var mockLoggerFactory = Substitute.For<ILoggerFactory>();
        var mockLogger = Substitute.For<ILogger>();
        mockLoggerFactory.CreateLogger(Arg.Any<string>()).Returns(mockLogger);
        serviceCollection.AddSingleton(mockLoggerFactory);

        return serviceCollection;
    }

    private void ConfigureAllDependenciesExcept(string skip)
    {
        if (skip != "Deserialize") _builder.WithDeserializer(_deserializer);
        if (skip != "Idempotency") _builder.WithIdempotency((_, _) => "test", (_, _) => DateTime.Now);
        if (skip != "Validate") _builder.WithValidator(_validator);
        if (skip != "Transform") _builder.WithTransformer(_transformer);
        if (skip != "Serialize") _builder.WithSerializer(_serializer);
        if (skip != "Sender") _builder.WithSender(_sender);
        if (skip != "Business incident route")
            _builder.WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST2"]
            );
    }

    private static ExtractorBuilder<int, int, Context> GetBuilder()
    {
        return ExtractorBuilder<int, int, Context>.Create("Test",
            GetServiceCollection().BuildServiceProvider());
    }

    [Fact]
    public void WithSenderFromServices_WhenSenderIsMissing_ThrowsException()
    {
        // Arrange
        var serviceProvider = GetServiceCollection().RemoveAll<SendStep<Context>>().BuildServiceProvider();
        var builder = ExtractorBuilder<int, int, Context>.Create("Test", serviceProvider);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => builder.WithSenderFromServices());
        Assert.Contains("SendStep", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithSenderFromServices_WithRegisteredSender_ConfiguresSender()
    {
        // Arrange
        var services = GetServiceCollection();
        services.AddSingleton<SendStep<Context>>(_sender);
        var builder = ExtractorBuilder<int, int, Context>.Create("Test", services.BuildServiceProvider());

        builder
            .WithDeserializer(_deserializer)
            .WithIdempotency((_, _) => "test", (_, _) => DateTime.Now)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSerializer(_serializer)
            .WithSenderFromServices()
            .WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST2"]);

        // Act
        var result = builder.Build();

        // Assert
        Assert.IsType<Extractor<int, int, Context>>(result);
    }

    [Fact]
    public void WithDaprTopicPublisher_WhenDaprClientIsMissing_ThrowsException()
    {
        // Arrange
        var serviceProvider = GetServiceCollection().RemoveAll<DaprClient>().BuildServiceProvider();
        var builder = ExtractorBuilder<int, int, Context>.Create("Test", serviceProvider);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            builder.WithDaprTopicPublisher("pubsub", "topic", new Uri("urn:test"), "test.type"));
        Assert.Contains("DaprClient", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithDaprTopicPublisher_WithValidParameters_ConfiguresSender()
    {
        // Arrange
        _builder
            .WithDeserializer(_deserializer)
            .WithIdempotency((_, _) => "test", (_, _) => DateTime.Now)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSerializer(_serializer)
            .WithDaprTopicPublisher("pubsub", "customers", new Uri("urn:test:crm"), "com.test.customer.extracted")
            .WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST2"]);

        // Act
        var result = _builder.Build();

        // Assert
        Assert.IsType<Extractor<int, int, Context>>(result);
    }

    [Theory]
    [InlineData(null, "topic", "urn:test", "type")]
    [InlineData("", "topic", "urn:test", "type")]
    [InlineData("pubsub", null, "urn:test", "type")]
    [InlineData("pubsub", "", "urn:test", "type")]
    [InlineData("pubsub", "topic", null, "type")]
    [InlineData("pubsub", "topic", "urn:test", null)]
    [InlineData("pubsub", "topic", "urn:test", "")]
    public void WithDaprTopicPublisher_WithInvalidParameters_ThrowsException(
        string? pubSubName,
        string? topicName,
        string? sourceUri,
        string? type)
    {
        // Arrange
        var source = sourceUri != null ? new Uri(sourceUri) : null;

        // Act & Assert
        Assert.ThrowsAny<ArgumentException>(() =>
            _builder.WithDaprTopicPublisher(pubSubName!, topicName!, source!, type!));
    }

    [Fact]
    public void WithDaprServiceInvoker_WhenDaprClientIsMissing_ThrowsException()
    {
        // Arrange
        var serviceProvider = GetServiceCollection().RemoveAll<DaprClient>().BuildServiceProvider();
        var builder = ExtractorBuilder<int, int, Context>.Create("Test", serviceProvider);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            builder.WithDaprServiceInvoker("reconciler", new Uri("urn:test"), "test.type", _ => new HttpClient()));
        Assert.Contains("DaprClient", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithDaprServiceInvoker_WithValidParameters_ConfiguresSender()
    {
        // Arrange
        _builder
            .WithDeserializer(_deserializer)
            .WithIdempotency((_, _) => "test", (_, _) => DateTime.Now)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSerializer(_serializer)
            .WithDaprServiceInvoker("reconciler", new Uri("urn:test:crm"), "com.test.customer.extracted", _ => new HttpClient())
            .WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST2"]);

        // Act
        var result = _builder.Build();

        // Assert
        Assert.IsType<Extractor<int, int, Context>>(result);
    }

    [Theory]
    [InlineData(null, "urn:test", "type")]
    [InlineData("", "urn:test", "type")]
    [InlineData("appId", null, "type")]
    [InlineData("appId", "urn:test", null)]
    [InlineData("appId", "urn:test", "")]
    public void WithDaprServiceInvoker_WithInvalidParameters_ThrowsException(
        string? appId,
        string? sourceUri,
        string? type)
    {
        // Arrange
        Uri? source = sourceUri != null ? new Uri(sourceUri) : null;

        // Act & Assert
        Assert.ThrowsAny<ArgumentException>(() =>
            _builder.WithDaprServiceInvoker(appId!, source!, type!, _ => new HttpClient()));
    }
}
