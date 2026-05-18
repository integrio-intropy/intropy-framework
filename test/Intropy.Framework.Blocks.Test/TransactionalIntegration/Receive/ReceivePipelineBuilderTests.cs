using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Receive;

public class ReceivePipelineBuilderTests
{
    private readonly ILoggerFactory _loggerFactory = Substitute.For<ILoggerFactory>();
    private readonly IBusinessIncidentServiceClient _businessIncidentServiceClient =
        Substitute.For<IBusinessIncidentServiceClient>();
    private static readonly FrameworkOptions FrameworkOptions = new() { ComponentName = "Test", ServiceNamespace = "Org" };

    private readonly ReceiveStep<ReceiveContext> _receiver = Substitute.For<ReceiveStep<ReceiveContext>>();
    private readonly EnqueueStep<ReceiveContext> _enqueuer = Substitute.For<EnqueueStep<ReceiveContext>>(FrameworkOptions);
    private readonly CompleteStep<ReceiveContext> _completer = Substitute.For<CompleteStep<ReceiveContext>>();

    [Fact]
    public void Create_WithNullPipelineName_ThrowsException()
    {
        string pipelineName = null!;

        Assert.Throws<ArgumentNullException>(() =>
            ReceivePipelineBuilder<ReceiveContext>.Create(pipelineName, FrameworkOptions, _loggerFactory));
    }

    [Fact]
    public void Create_WithEmptyPipelineName_ThrowsException()
    {
        const string pipelineName = "";

        Assert.Throws<ArgumentException>(() =>
            ReceivePipelineBuilder<ReceiveContext>.Create(pipelineName, FrameworkOptions, _loggerFactory));
    }

    [Fact]
    public void Create_WithNullFrameworkOptions_ThrowsException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            ReceivePipelineBuilder<ReceiveContext>.Create("Test", null!, _loggerFactory));
    }

    [Fact]
    public void Create_WithNullLoggerFactory_ThrowsException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            ReceivePipelineBuilder<ReceiveContext>.Create("Test", FrameworkOptions, null!));
    }

    [Fact]
    public void Create_WithValidArguments_CreatesBuilder()
    {
        var result = ReceivePipelineBuilder<ReceiveContext>.Create("Test", FrameworkOptions, _loggerFactory);

        Assert.IsType<ReceivePipelineBuilder<ReceiveContext>>(result);
    }

    [Theory]
    [InlineData(typeof(ReceiveStep<>))]
    [InlineData(typeof(EnqueueStep<>))]
    [InlineData(typeof(CompleteStep<>))]
    [InlineData(typeof(BusinessIncidentRouteStep<,>))]
    public void Build_WhenDependencyMissing_ThrowsInvalidOperationException(Type missingDependency)
    {
        var builder = ConfigureAllDependenciesExcept(missingDependency);
        var exceptionMessageMatch = missingDependency.Name.Split('`').First();

        var exception = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains(exceptionMessageMatch, exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_WithAllDependencies_CreatesPipeline()
    {
        var builder = GetBuilder()
            .WithReceiver(_receiver)
            .WithEnqueuer(_enqueuer)
            .WithCompleter(_completer)
            .WithBusinessIncidents(_businessIncidentServiceClient, ctx => ctx.Metadata["sourceItemId"],
                ctx => ctx.Metadata["sourceItemId"]);

        var result = builder.Build();

        Assert.IsType<ReceivePipeline<ReceiveContext>>(result);
    }

    private ReceivePipelineBuilder<ReceiveContext> ConfigureAllDependenciesExcept(Type skip)
    {
        var builder = GetBuilder();

        if (skip != typeof(ReceiveStep<>))
            builder.WithReceiver(_receiver);

        if (skip != typeof(EnqueueStep<>))
            builder.WithEnqueuer(_enqueuer);

        if (skip != typeof(CompleteStep<>))
            builder.WithCompleter(_completer);

        if (skip != typeof(BusinessIncidentRouteStep<,>))
            builder.WithBusinessIncidents(_businessIncidentServiceClient, ctx => ctx.Metadata["sourceItemId"],
                ctx => ctx.Metadata["sourceItemId"]);

        return builder;
    }

    private ReceivePipelineBuilder<ReceiveContext> GetBuilder()
    {
        return ReceivePipelineBuilder<ReceiveContext>.Create("Test", FrameworkOptions, _loggerFactory);
    }
}
