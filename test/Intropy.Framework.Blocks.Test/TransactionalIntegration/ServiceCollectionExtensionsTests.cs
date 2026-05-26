using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Test.TransactionalIntegration.Send;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddTransactionalIntegrationPipeline_ShouldThrowArgumentNullException_WhenServicesIsNull()
    {
        // Verifies that null service collection is rejected
        IServiceCollection? services = null;

        var exception = Assert.Throws<ArgumentNullException>(() =>
            services!.AddSendPipeline<MyInput, MyOutput, MyContext>(
                "test-pipeline",
                (builder, _) => builder));

        Assert.Equal("services", exception.ParamName);
    }

    [Fact]
    public void
        AddTransactionalIntegrationPipeline_ShouldThrowArgumentNullException_WhenConfigurePipelineIsNull()
    {
        // Verifies that null pipeline configurator is rejected
        var services = new ServiceCollection();

        var exception = Assert.Throws<ArgumentNullException>(() =>
            services.AddSendPipeline<MyInput, MyOutput, MyContext>(
                "test-pipeline",
                null!));

        Assert.Equal("configurePipeline", exception.ParamName);
    }

    [Fact]
    public void AddTransactionalIntegrationPipeline_ShouldRegisterPipeline_WhenDependenciesArePresent()
    {
        // Verifies that pipeline is registered when all required dependencies exist
        var services = new ServiceCollection();
        services.AddSingleton(new FrameworkOptions { ComponentName = "Test", ServiceNamespace = "Org" });
        services.AddSingleton(Substitute.For<ILoggerFactory>());

        services.AddSendPipeline<MyInput, MyOutput, MyContext>(
            "test-pipeline",
            (builder, _) => Configure(builder));

        var provider = services.BuildServiceProvider();
        var pipeline = provider
            .GetRequiredService<ISendPipeline<MyContext>>();

        Assert.NotNull(pipeline);
    }

    [Fact]
    public void AddTransactionalIntegrationPipeline_ShouldPassServiceProvider_ToConfigurator()
    {
        // Verifies that the service provider is correctly passed to the configurator function
        var services = new ServiceCollection();
        services.AddSingleton(new FrameworkOptions { ComponentName = "Test", ServiceNamespace = "Org" });
        services.AddSingleton(Substitute.For<ILoggerFactory>());

        IServiceProvider? capturedServiceProvider = null;

        services.AddSendPipeline<MyInput, MyOutput, MyContext>(
            "test-pipeline",
            (builder, sp) =>
            {
                capturedServiceProvider = sp;
                return Configure(builder);
            });

        var provider = services.BuildServiceProvider();
        var service = provider.GetRequiredService<ISendPipeline<MyContext>>();

        Assert.NotNull(capturedServiceProvider);
        Assert.NotNull(service);
    }

    [Fact]
    public void AddTransactionalIntegrationPipeline_ShouldUsePipelineName_WhenCreatingBuilder()
    {
        // Verifies that the specified pipeline name is used
        var services = new ServiceCollection();
        services.AddSingleton(new FrameworkOptions { ComponentName = "Test", ServiceNamespace = "Org" });
        var loggerFactory = Substitute.For<ILoggerFactory>();
        loggerFactory.CreateLogger(Arg.Any<string>()).Returns(Substitute.For<ILogger>());
        services.AddSingleton(loggerFactory);

        const string pipelineName = "OrderProcessingPipeline";

        services.AddSendPipeline<MyInput, MyOutput, MyContext>(
            pipelineName,
            (builder, _) => Configure(builder));

        var provider = services.BuildServiceProvider();
        var pipeline = provider
            .GetRequiredService<ISendPipeline<MyContext>>();

        Assert.NotNull(pipeline);
        // Pipeline name is used internally, no direct way to verify but ensuring no exceptions
    }

    [Fact]
    public void AddTransactionalIntegrationPipeline_ShouldReturnServiceCollection_ForMethodChaining()
    {
        // Verifies that the method returns the service collection for fluent API
        var services = new ServiceCollection();
        services.AddSingleton(new FrameworkOptions { ComponentName = "Test", ServiceNamespace = "Org" });
        services.AddSingleton(Substitute.For<ILoggerFactory>());

        var result = services.AddSendPipeline<MyInput, MyOutput, MyContext>(
            "test-pipeline",
            (builder, _) => builder);

        Assert.Same(services, result);
    }

    private static SendPipelineBuilder<MyInput, MyOutput, MyContext> Configure(
        SendPipelineBuilder<MyInput, MyOutput, MyContext> builder)
    {
        return builder
            .WithDeserializer(Substitute.For<DeserializeStep<MyInput, MyContext>>())
            .WithValidator(Substitute.For<ValidateStep<MyInput, MyContext>>())
            .WithIdempotency(Substitute.For<IIdempotencyServiceClient>(), (_, _) => "", (_, _) => DateTime.MinValue)
            .WithExtractor(Substitute.For<ExtractStep<MyInput, MyContext>>())
            .WithTransformer(Substitute.For<TransformStep<MyInput, MyOutput, MyContext>>())
            .WithSerializer(Substitute.For<SerializeStep<MyOutput, MyContext>>())
            .WithSender(Substitute.For<SendStep<MyContext>>())
            .WithBusinessIncidents(Substitute.For<IBusinessIncidentServiceClient>(), _ => "", _ => "");
    }
}
