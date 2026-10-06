using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.Loader;

/// <summary>
/// The generic, DI-resolving step overloads on the loader builder
/// (<c>WithDeserializer&lt;TStep&gt;()</c> and friends): the step is resolved from the provider the
/// builder was created with, so each route's pipeline can use its own step type, registered in DI,
/// with no cast-and-resolve boilerplate in the route lambda. A missing registration fails with the
/// builders' own missing-dependency error, naming the step and how to register it.
/// </summary>
public class LoaderBuilderStepResolutionTests
{
    [Fact]
    public void MissingStep_NamesTheStepAndTheRemedy()
    {
        var builder = LoaderBuilder<int, int, Context>.Create("Test", GetServiceCollection().BuildServiceProvider());

        var deserializer = Assert.Throws<InvalidOperationException>(() => builder.WithDeserializer<DeserializeStep<int, Context>>());
        var validator = Assert.Throws<InvalidOperationException>(() => builder.WithValidator<ValidateStep<int, Context>>());
        var transformer = Assert.Throws<InvalidOperationException>(() => builder.WithTransformer<TransformStep<int, int, Context>>());
        var extractor = Assert.Throws<InvalidOperationException>(() => builder.WithExtractor<ExtractStep<int, Context>>());
        var sender = Assert.Throws<InvalidOperationException>(() => builder.WithSender<SendStep<int, Context>>());

        Assert.Contains("not registered in the service provider", deserializer.Message);
        Assert.Contains("Please register it using services.AddScoped", deserializer.Message);
        Assert.Contains("not registered in the service provider", validator.Message);
        Assert.Contains("not registered in the service provider", transformer.Message);
        Assert.Contains("not registered in the service provider", extractor.Message);
        Assert.Contains("not registered in the service provider", sender.Message);
    }

    [Fact]
    public void RegisteredSteps_AreResolvedFromServices_AndThePipelineBuilds()
    {
        var pipeline = LoaderBuilder<int, int, Context>.Create("Test", GetServiceCollection()
                .AddSingleton(Substitute.For<DeserializeStep<int, Context>>())
                .AddSingleton(Substitute.For<ValidateStep<int, Context>>())
                .AddSingleton(Substitute.For<TransformStep<int, int, Context>>())
                .AddSingleton(Substitute.For<ExtractStep<int, Context>>())
                .AddSingleton(Substitute.For<SendStep<int, Context>>())
                .BuildServiceProvider())
            .WithDeserializer<DeserializeStep<int, Context>>()
            .WithIdempotency()
            .WithValidator<ValidateStep<int, Context>>()
            .WithTransformer<TransformStep<int, int, Context>>()
            .WithExtractor<ExtractStep<int, Context>>()
            .WithSender<SendStep<int, Context>>()
            .WithBusinessIncidents(_ => "test", _ => "test");

        // All required steps configured, the last one through DI: the pipeline builds.
        Assert.NotNull(pipeline.Build());
    }

    [Fact]
    public void ScopedSteps_AreResolvedInTheScopeTheBuilderWasCreatedIn()
    {
        // A hosted loader builds the pipeline in each message's own scope: a scoped registration
        // must give each scope its own step instance.
        var created = new List<DeserializeStep<int, Context>>();
        var services = GetServiceCollection();
        services.AddScoped<DeserializeStep<int, Context>>(_ =>
        {
            var step = Substitute.For<DeserializeStep<int, Context>>();
            created.Add(step);
            return step;
        });
        var provider = services.BuildServiceProvider();

        using var scope1 = provider.CreateScope();
        LoaderBuilder<int, int, Context>.Create("Test", scope1.ServiceProvider)
            .WithDeserializer<DeserializeStep<int, Context>>();
        using var scope2 = provider.CreateScope();
        LoaderBuilder<int, int, Context>.Create("Test", scope2.ServiceProvider)
            .WithDeserializer<DeserializeStep<int, Context>>();

        Assert.Equal(2, created.Count);
        Assert.NotSame(created[0], created[1]);
    }

    private static ServiceCollection GetServiceCollection()
    {
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddSingleton(Substitute.For<IIdempotencyServiceClient>());
        serviceCollection.AddSingleton(Substitute.For<IBusinessIncidentServiceClient>());
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
}
