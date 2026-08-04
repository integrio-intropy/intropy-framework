using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.Loader;

public class LoaderBuilderTests
{
    private readonly LoaderBuilder<int, int, Context> _builder = GetBuilder();
    private readonly DeserializeStep<int, Context> _deserializer = Substitute.For<DeserializeStep<int, Context>>();
    private readonly ValidateStep<int, Context> _validator = Substitute.For<ValidateStep<int, Context>>();
    private readonly TransformStep<int, int, Context> _transformer = Substitute.For<TransformStep<int, int, Context>>();
    private readonly SendStep<int, Context> _sender = Substitute.For<SendStep<int, Context>>();

    [Fact]
    public void Create_WithNullServiceProvider_ThrowsException()
    {
        // Arrange
        IServiceProvider serviceProvider = null!;
        const string pipelineName = "Test";

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            LoaderBuilder<int, int, Context>.Create(pipelineName, serviceProvider));
    }

    [Fact]
    public void Create_WithNullPipelineName_ThrowsException()
    {
        // Arrange
        IServiceProvider serviceProvider = new ServiceCollection().BuildServiceProvider();
        string pipelineName = null!;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            LoaderBuilder<int, int, Context>.Create(pipelineName, serviceProvider));
    }

    [Fact]
    public void Create_WithEmptyPipelineName_ThrowsException()
    {
        // Arrange
        IServiceProvider serviceProvider = new ServiceCollection().BuildServiceProvider();
        const string pipelineName = "";

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            LoaderBuilder<int, int, Context>.Create(pipelineName, serviceProvider));
    }

    [Fact]
    public void Create_WithNoRegisteredILoggerFactory_ThrowsException()
    {
        var serviceProvider = GetServiceCollection().RemoveAll<ILoggerFactory>().BuildServiceProvider();
        const string pipelineName = "Test";

        // Act & assert
        Assert.Throws<InvalidOperationException>(() =>
            LoaderBuilder<int, int, Context>.Create(pipelineName, serviceProvider));
    }

    [Fact]
    public void Create_WithNoRegisteredFrameworkOptions_ThrowsException()
    {
        // Arrange
        var serviceProvider = GetServiceCollection().RemoveAll<FrameworkOptions>().BuildServiceProvider();
        const string pipelineName = "Test";

        // Act & assert
        Assert.Throws<InvalidOperationException>(() =>
            LoaderBuilder<int, int, Context>.Create(pipelineName, serviceProvider));
    }

    [Theory]
    [InlineData("Deserialize")]
    [InlineData("Idempotency")]
    [InlineData("Validate")]
    [InlineData("Transform")]
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
    public void Build_WithAllDependencies_CreatesLoader()
    {
        // Arrange
        _builder
            .WithDeserializer(_deserializer)
            .WithIdempotency()
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSender(_sender)
            .WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST"]);

        // Act
        var result = _builder.Build();

        // Assert
        Assert.IsType<Loader<int, int, Context>>(result);
    }

    [Fact]
    public void WithIdempotency_WhenIdempotencyServiceClientIsMissing_ThrowsException()
    {
        // Arrange
        var serviceProvider = GetServiceCollection().RemoveAll<IIdempotencyServiceClient>().BuildServiceProvider();
        var builder = LoaderBuilder<int, int, Context>.Create("Test", serviceProvider);

        // Act & assert
        Assert.Throws<InvalidOperationException>(() => builder.WithIdempotency());
    }

    [Fact]
    public void WithBusinessIncidents_WhenBusinessIncidentServiceClientIsMissing_ThrowsException()
    {
        // Arrange
        var serviceProvider = GetServiceCollection().RemoveAll<IBusinessIncidentServiceClient>().BuildServiceProvider();
        var builder = LoaderBuilder<int, int, Context>.Create("Test", serviceProvider);

        // Act & assert
        Assert.Throws<InvalidOperationException>(() => builder.WithBusinessIncidents(x => x.Metadata["TEST"],
            x => x.Metadata["TEST"]));
    }

    [Fact]
    public void WithIdempotency_WithCustomHashGenerator_ConfiguresIdempotency()
    {
        // Arrange
        _builder
            .WithDeserializer(_deserializer)
            .WithIdempotency(hashGenerator: _ => "custom-hash")
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSender(_sender)
            .WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST"]);

        // Act
        var result = _builder.Build();

        // Assert
        Assert.IsType<Loader<int, int, Context>>(result);
    }

    [Fact]
    public void WithSenderFromServices_WhenSenderIsMissing_ThrowsException()
    {
        // Arrange
        var serviceProvider = GetServiceCollection().RemoveAll<SendStep<int, Context>>().BuildServiceProvider();
        var builder = LoaderBuilder<int, int, Context>.Create("Test", serviceProvider);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => builder.WithSenderFromServices());
        Assert.Contains("SendStep", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithSenderFromServices_WithRegisteredSender_ConfiguresSender()
    {
        // Arrange
        var services = GetServiceCollection();
        services.AddSingleton<SendStep<int, Context>>(_sender);
        var builder = LoaderBuilder<int, int, Context>.Create("Test", services.BuildServiceProvider());

        builder
            .WithDeserializer(_deserializer)
            .WithIdempotency()
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithSenderFromServices()
            .WithBusinessIncidents(x => x.Metadata["TEST"],
                x => x.Metadata["TEST"]);

        // Act
        var result = builder.Build();

        // Assert
        Assert.IsType<Loader<int, int, Context>>(result);
    }

    private static ServiceCollection GetServiceCollection()
    {
        var idempotencyServiceClient = Substitute.For<IIdempotencyServiceClient>();
        var businessIncidentServiceClient = Substitute.For<IBusinessIncidentServiceClient>();

        var serviceCollection = new ServiceCollection();
        serviceCollection.AddSingleton(idempotencyServiceClient);
        serviceCollection.AddSingleton(businessIncidentServiceClient);
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
        if (skip != "Idempotency") _builder.WithIdempotency();
        if (skip != "Validate") _builder.WithValidator(_validator);
        if (skip != "Transform") _builder.WithTransformer(_transformer);
        if (skip != "Sender") _builder.WithSender(_sender);
        if (skip != "Business incident route") _builder.WithBusinessIncidents(x => x.Metadata["TEST"],
            x => x.Metadata["TEST"]);
    }

    private static LoaderBuilder<int, int, Context> GetBuilder()
    {
        return LoaderBuilder<int, int, Context>.Create("Test",
            GetServiceCollection().BuildServiceProvider());
    }
}
