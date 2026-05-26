using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Receive;

public class ReceivePipelineTests
{
    private readonly IBusinessIncidentServiceClient _businessIncidentServiceClient;
    private readonly FrameworkOptions _frameworkOptions = new() { ComponentName = "Test",  ServiceNamespace = "Org" };
    private readonly ILoggerFactory _loggerFactory = Substitute.For<ILoggerFactory>();

    public ReceivePipelineTests()
    {
        _businessIncidentServiceClient = Substitute.For<IBusinessIncidentServiceClient>();
        _businessIncidentServiceClient.Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
                Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>())
            .ReturnsForAnyArgs(Task.CompletedTask);

        var mockLogger = Substitute.For<ILogger>();
        _loggerFactory.CreateLogger(Arg.Any<string>()).Returns(mockLogger);
    }

    [Fact]
    public async Task Execute_WithSuccessfulSteps_ReturnsSuccess()
    {
        // Arrange
        var pipeline = GetPipelineBuilder()
            .WithReceiver(new SuccessfulReceiver())
            .WithEnqueuer(new SuccessfulEnqueuer(_frameworkOptions))
            .WithCompleter(new SuccessfulCompleter())
            .WithBusinessIncidents(_businessIncidentServiceClient, ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"),
                ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"))
            .Build();

        var itemInfo = new SourceItemInfo("test-file.txt");
        var context = ReceiveContext.Create();

        // Act
        var (result, _) = await pipeline.Execute(itemInfo, context);

        // Assert
        Assert.IsType<StepResult<SourceItem>.Success>(result);
        // Note: The BusinessIncidentRouter finalizer returns a default value on success,
        // not the original value from the pipeline. This is the expected behavior.
    }

    [Fact]
    public async Task Execute_WhenReceiveFails_RoutesBusinessIncident()
    {
        // Arrange
        var pipeline = GetPipelineBuilder()
            .WithReceiver(new FailingReceiver())
            .WithEnqueuer(new SuccessfulEnqueuer(_frameworkOptions))
            .WithCompleter(new SuccessfulCompleter())
            .WithBusinessIncidents(_businessIncidentServiceClient, ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"),
                ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"))
            .Build();

        var itemInfo = new SourceItemInfo("missing-file.txt");
        var context = ReceiveContext.Create();
        context.Metadata["sourceItemId"] = "missing-file.txt";

        // Act
        var (result, _) = await pipeline.Execute(itemInfo, context);

        // Assert - business incident routed, so result becomes Success (incident was handled)
        Assert.IsType<StepResult<SourceItem>.Success>(result);
        await _businessIncidentServiceClient.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Execute_WhenCompleteFails_RoutesBusinessIncident()
    {
        // Arrange
        var pipeline = GetPipelineBuilder()
            .WithReceiver(new SuccessfulReceiver())
            .WithEnqueuer(new SuccessfulEnqueuer(_frameworkOptions))
            .WithCompleter(new FailingCompleter())
            .WithBusinessIncidents(_businessIncidentServiceClient, ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"),
                ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"))
            .Build();

        var itemInfo = new SourceItemInfo("test-file.txt");
        var context = ReceiveContext.Create();
        context.Metadata["sourceItemId"] = "test-file.txt";

        // Act
        var (result, _) = await pipeline.Execute(itemInfo, context);

        // Assert - business incident routed for complete failure
        Assert.IsType<StepResult<SourceItem>.Success>(result);
        await _businessIncidentServiceClient.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Execute_WithoutBusinessIncidents_ProcessesSuccessfully()
    {
        // Arrange
        var pipeline = GetPipelineBuilder()
            .WithReceiver(new SuccessfulReceiver())
            .WithEnqueuer(new SuccessfulEnqueuer(_frameworkOptions))
            .WithCompleter(new SuccessfulCompleter())
            .Build();

        var itemInfo = new SourceItemInfo("test-file.txt");
        var context = ReceiveContext.Create();

        // Act
        var (result, _) = await pipeline.Execute(itemInfo, context);

        // Assert
        Assert.IsType<StepResult<SourceItem>.Success>(result);
        await _businessIncidentServiceClient.DidNotReceiveWithAnyArgs().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Execute_WithoutBusinessIncidents_WhenReceiveFails_ReturnsBusinessFailure()
    {
        // Arrange
        var pipeline = GetPipelineBuilder()
            .WithReceiver(new FailingReceiver())
            .WithEnqueuer(new SuccessfulEnqueuer(_frameworkOptions))
            .WithCompleter(new SuccessfulCompleter())
            .Build();

        var itemInfo = new SourceItemInfo("missing-file.txt");
        var context = ReceiveContext.Create();

        // Act
        var (result, _) = await pipeline.Execute(itemInfo, context);

        // Assert - no router means the failure surfaces instead of being routed
        Assert.IsType<StepResult<SourceItem>.BusinessFailure>(result);
        await _businessIncidentServiceClient.DidNotReceiveWithAnyArgs().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Execute_ProcessesMultipleItems()
    {
        // Arrange
        var pipeline = GetPipelineBuilder()
            .WithReceiver(new SuccessfulReceiver())
            .WithEnqueuer(new SuccessfulEnqueuer(_frameworkOptions))
            .WithCompleter(new SuccessfulCompleter())
            .WithBusinessIncidents(_businessIncidentServiceClient, ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"),
                ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"))
            .Build();

        var items = new[]
        {
            new SourceItemInfo("file1.txt"),
            new SourceItemInfo("file2.txt"),
            new SourceItemInfo("file3.txt")
        };

        // Act & Assert
        foreach (var item in items)
        {
            var context = ReceiveContext.Create();
            var (result, _) = await pipeline.Execute(item, context);
            Assert.IsType<StepResult<SourceItem>.Success>(result);
        }
    }

    private ReceivePipelineBuilder<ReceiveContext> GetPipelineBuilder()
    {
        return ReceivePipelineBuilder<ReceiveContext>.Create("TestReceivePipeline", _frameworkOptions, _loggerFactory);
    }
}
