using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.BulkLoader;
using Intropy.Framework.Blocks.BulkLoader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.BulkLoader;

public class BulkLoaderBuilderTests
{
    private readonly BulkLoaderBuilder<int, int, Context> _builder = GetBuilder();
    private readonly DeserializeStep<int, Context> _deserializer = Substitute.For<DeserializeStep<int, Context>>();
    private readonly ValidateStep<int, Context> _validator = Substitute.For<ValidateStep<int, Context>>();
    private readonly TransformStep<int, int, Context> _transformer = Substitute.For<TransformStep<int, int, Context>>();
    private readonly BatchSendStep<int, Context> _sender = Substitute.For<BatchSendStep<int, Context>>();

    [Fact]
    public void Create_WithNullServiceProvider_ThrowsException()
    {
        IServiceProvider serviceProvider = null!;
        Assert.Throws<ArgumentNullException>(() =>
            BulkLoaderBuilder<int, int, Context>.Create("Test", serviceProvider));
    }

    [Fact]
    public void Create_WithEmptyPipelineName_ThrowsException()
    {
        IServiceProvider serviceProvider = new ServiceCollection().BuildServiceProvider();
        Assert.Throws<ArgumentException>(() =>
            BulkLoaderBuilder<int, int, Context>.Create("", serviceProvider));
    }

    [Fact]
    public void Create_WithNoRegisteredILoggerFactory_ThrowsException()
    {
        var serviceProvider = GetServiceCollection().RemoveAll<ILoggerFactory>().BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() =>
            BulkLoaderBuilder<int, int, Context>.Create("Test", serviceProvider));
    }

    [Fact]
    public void Create_WithNoRegisteredFrameworkOptions_ThrowsException()
    {
        var serviceProvider = GetServiceCollection().RemoveAll<FrameworkOptions>().BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() =>
            BulkLoaderBuilder<int, int, Context>.Create("Test", serviceProvider));
    }

    [Theory]
    [InlineData("Deserialize")]
    [InlineData("Validate")]
    [InlineData("Transform")]
    [InlineData("Sender")]
    [InlineData("Business incident route")]
    public void Build_WhenDependencyMissing_ThrowsInvalidOperationException(string missingDependency)
    {
        ConfigureAllDependenciesExcept(missingDependency);

        var exception = Assert.Throws<InvalidOperationException>(() => _builder.Build());
        Assert.Contains(missingDependency, exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_WithAllDependencies_CreatesBulkLoader()
    {
        _builder
            .WithDeserializer(_deserializer)
            .WithIdempotency((_, _) => "id", (_, _) => DateTimeOffset.UtcNow)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithBatchSender(_sender)
            .WithBusinessIncidents(x => x.Metadata["TEST"], x => x.Metadata["TEST"]);

        var result = _builder.Build();

        Assert.IsType<BulkLoader<int, int, Context>>(result);
    }

    [Fact]
    public void Build_WithoutIdempotency_CreatesBulkLoader_WithNoIdempotencyServiceClientRegistered()
    {
        var serviceProvider = GetServiceCollection().RemoveAll<IIdempotencyServiceClient>().BuildServiceProvider();
        var builder = BulkLoaderBuilder<int, int, Context>.Create("Test", serviceProvider);
        ConfigureAllDependenciesExcept("Idempotency", builder);

        var result = builder.Build();

        Assert.IsType<BulkLoader<int, int, Context>>(result);
    }

    [Fact]
    public void WithIdempotency_WhenIdempotencyServiceClientIsMissing_ThrowsException()
    {
        var serviceProvider = GetServiceCollection().RemoveAll<IIdempotencyServiceClient>().BuildServiceProvider();
        var builder = BulkLoaderBuilder<int, int, Context>.Create("Test", serviceProvider);

        Assert.Throws<InvalidOperationException>(() =>
            builder.WithIdempotency((_, _) => "id", (_, _) => DateTimeOffset.UtcNow));
    }

    [Fact]
    public void WithIdempotency_WithEmptyId_ThrowsOnlyOnceTheExtractorIsInvoked()
    {
        _builder
            .WithDeserializer(_deserializer)
            .WithIdempotency((_, _) => "", (_, _) => DateTimeOffset.UtcNow)
            .WithValidator(_validator)
            .WithTransformer(_transformer)
            .WithBatchSender(_sender)
            .WithBusinessIncidents(x => x.Metadata["TEST"], x => x.Metadata["TEST"]);

        // Building succeeds — the guard only fires when the pipeline actually runs the extractor.
        Assert.IsType<BulkLoader<int, int, Context>>(_builder.Build());
    }

    [Fact]
    public void WithBusinessIncidents_WhenBusinessIncidentServiceClientIsMissing_ThrowsException()
    {
        var serviceProvider = GetServiceCollection().RemoveAll<IBusinessIncidentServiceClient>().BuildServiceProvider();
        var builder = BulkLoaderBuilder<int, int, Context>.Create("Test", serviceProvider);

        Assert.Throws<InvalidOperationException>(() =>
            builder.WithBusinessIncidents(x => x.Metadata["TEST"], x => x.Metadata["TEST"]));
    }

    [Fact]
    public void WithSenderFromServices_WhenSenderIsMissing_ThrowsException()
    {
        var builder = BulkLoaderBuilder<int, int, Context>.Create("Test", GetServiceCollection().BuildServiceProvider());

        var exception = Assert.Throws<InvalidOperationException>(() => builder.WithSenderFromServices());
        Assert.Contains("SendStep", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("BatchSendStep", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithBatchSenderFromServices_WhenSenderIsMissing_ThrowsException()
    {
        var builder = BulkLoaderBuilder<int, int, Context>.Create("Test", GetServiceCollection().BuildServiceProvider());

        var exception = Assert.Throws<InvalidOperationException>(() => builder.WithBatchSenderFromServices());
        Assert.Contains("BatchSendStep", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithSenderFromServices_WithRegisteredSender_ConfiguresSender()
    {
        var services = GetServiceCollection();
        services.AddSingleton(Substitute.For<SendStep<int, Context>>());
        var builder = BulkLoaderBuilder<int, int, Context>.Create("Test", services.BuildServiceProvider());

        ConfigureAllDependenciesExcept("Sender", builder);
        builder.WithSenderFromServices();

        Assert.IsType<BulkLoader<int, int, Context>>(builder.Build());
    }

    [Fact]
    public void WithBatchSenderFromServices_WithRegisteredSender_ConfiguresSender()
    {
        var services = GetServiceCollection();
        services.AddSingleton(_sender);
        var builder = BulkLoaderBuilder<int, int, Context>.Create("Test", services.BuildServiceProvider());

        ConfigureAllDependenciesExcept("Sender", builder);
        builder.WithBatchSenderFromServices();

        Assert.IsType<BulkLoader<int, int, Context>>(builder.Build());
    }

    [Fact]
    public void WithBatchExtractor_OfARegisteredType_ResolvesIt()
    {
        var services = GetServiceCollection();
        services.AddSingleton(Substitute.For<BatchExtractStep<int, Context>>());
        var builder = BulkLoaderBuilder<int, int, Context>.Create("Test", services.BuildServiceProvider());

        ConfigureAllDependenciesExcept("", builder);
        builder.WithBatchExtractor<BatchExtractStep<int, Context>>();

        Assert.IsType<BulkLoader<int, int, Context>>(builder.Build());
    }

    [Fact]
    public void WithBatchExtractor_OfAnUnregisteredType_Throws()
    {
        var builder = GetBuilder();

        Assert.Throws<InvalidOperationException>(() => builder.WithBatchExtractor<BatchExtractStep<int, Context>>());
    }

    [Fact]
    public void Build_WithoutASender_NamesBothWaysToConfigureOne()
    {
        ConfigureAllDependenciesExcept("Sender");

        var exception = Assert.Throws<InvalidOperationException>(() => _builder.Build());
        Assert.Contains("WithSender()", exception.Message, StringComparison.Ordinal);
        Assert.Contains("WithBatchSender()", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void WithParallelism_BelowOne_Throws(int degree)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => _builder.WithParallelism(degree));
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

    private void ConfigureAllDependenciesExcept(string skip) => ConfigureAllDependenciesExcept(skip, _builder);

    private void ConfigureAllDependenciesExcept(string skip,
        BulkLoaderBuilder<int, int, Context> builder)
    {
        if (skip != "Deserialize") builder.WithDeserializer(_deserializer);
        if (skip != "Idempotency") builder.WithIdempotency((_, _) => "id", (_, _) => DateTimeOffset.UtcNow);
        if (skip != "Validate") builder.WithValidator(_validator);
        if (skip != "Transform") builder.WithTransformer(_transformer);
        if (skip != "Sender") builder.WithBatchSender(_sender);
        if (skip != "Business incident route")
            builder.WithBusinessIncidents(x => x.Metadata["TEST"], x => x.Metadata["TEST"]);
    }

    private static BulkLoaderBuilder<int, int, Context> GetBuilder() =>
        BulkLoaderBuilder<int, int, Context>.Create("Test",
            GetServiceCollection().BuildServiceProvider());
}
