using System.Diagnostics;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Receive;

/// <summary>
/// The receive pipeline publishes one source item; the file sweep that runs it reads the source
/// before and completes it after. A failed publish must surface as a failure, so the sweep keeps
/// the source.
/// </summary>
public class ReceivePipelineTests
{
    // PipelineTracing (Intropy.Framework.Core) starts the pipeline spans.
    private const string PipelineActivitySource = "Intropy.Framework.Core";

    private readonly IBusinessIncidentServiceClient _businessIncidentServiceClient;
    private readonly FrameworkOptions _frameworkOptions = new() { ComponentName = "Test", ServiceNamespace = "Org" };
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
    public async Task Execute_WithSuccessfulEnqueue_ReturnsSuccessAndPublishesTheItem()
    {
        // Arrange
        var enqueuer = new SuccessfulEnqueuer(_frameworkOptions);
        var pipeline = GetPipelineBuilder()
            .WithEnqueuer(enqueuer)
            .WithBusinessIncidents(_businessIncidentServiceClient, ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"),
                ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"))
            .Build();

        // Act
        var (result, _) = await pipeline.Execute(Item("test-file.txt"), ReceiveContext.Create());

        // Assert
        Assert.IsType<StepResult<SourceItem>.Success>(result);
        Assert.Equal("test-file.txt", Assert.Single(enqueuer.Published).Id);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Execute_TracesAsItsOwnTraceLinkedToTheCaller_OnlyWhenDetached(bool detachTrace)
    {
        // Verifies that a detached execution starts a new trace linked to the caller's span, and
        // an attached one continues the caller's trace
        using var parentSource = new ActivitySource($"receive-test-{Guid.NewGuid()}");
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source == parentSource || source.Name == PipelineActivitySource,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (activities) activities.Add(activity); }
        };
        ActivitySource.AddActivityListener(listener);
        var pipeline = GetPipelineBuilder().WithEnqueuer(new SuccessfulEnqueuer(_frameworkOptions)).Build();

        Activity parent;
        using (parent = parentSource.StartActivity("job")!)
            await pipeline.Execute(Item("test-file.txt"), ReceiveContext.Create(), detachTrace);

        Activity[] captured;
        lock (activities) captured = [.. activities];
        var execution = Assert.Single(captured, a => a.DisplayName.StartsWith("Pipeline.", StringComparison.Ordinal) &&
            (a.TraceId == parent.TraceId || a.Links.Any(l => l.Context.SpanId == parent.SpanId)));
        Assert.Equal(detachTrace, execution.TraceId != parent.TraceId);
        Assert.Equal(detachTrace, execution.Links.Any(l => l.Context.SpanId == parent.SpanId));
    }

    [Fact]
    public async Task Execute_WhenEnqueueFails_ReturnsTechnicalFailureWithoutRoutingAnIncident()
    {
        // Arrange
        var pipeline = GetPipelineBuilder()
            .WithEnqueuer(new FailingEnqueuer(_frameworkOptions))
            .WithBusinessIncidents(_businessIncidentServiceClient, ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"),
                ctx => ctx.Metadata.GetValueOrDefault("sourceItemId", "unknown"))
            .Build();

        // Act
        var (result, _) = await pipeline.Execute(Item("test-file.txt"), ReceiveContext.Create());

        // Assert - a technical failure is not a business incident; the sweep keeps the source
        Assert.IsType<StepResult<SourceItem>.TechnicalFailure>(result);
        await _businessIncidentServiceClient.DidNotReceiveWithAnyArgs().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Execute_WithoutBusinessIncidents_ProcessesSuccessfully()
    {
        // Arrange
        var pipeline = GetPipelineBuilder()
            .WithEnqueuer(new SuccessfulEnqueuer(_frameworkOptions))
            .Build();

        // Act
        var (result, _) = await pipeline.Execute(Item("test-file.txt"), ReceiveContext.Create());

        // Assert
        Assert.IsType<StepResult<SourceItem>.Success>(result);
    }

    [Fact]
    public async Task Execute_ProcessesMultipleItems()
    {
        // Arrange
        var enqueuer = new SuccessfulEnqueuer(_frameworkOptions);
        var pipeline = GetPipelineBuilder().WithEnqueuer(enqueuer).Build();

        // Act
        foreach (var name in new[] { "file1.txt", "file2.txt", "file3.txt" })
        {
            var (result, _) = await pipeline.Execute(Item(name), ReceiveContext.Create());
            Assert.IsType<StepResult<SourceItem>.Success>(result);
        }

        // Assert
        Assert.Equal(["file1.txt", "file2.txt", "file3.txt"], enqueuer.Published.Select(item => item.Id));
    }

    private static SourceItem Item(string id) => new(id, "test-content"u8.ToArray());

    private ReceivePipelineBuilder<ReceiveContext> GetPipelineBuilder()
    {
        return ReceivePipelineBuilder<ReceiveContext>.Create("TestReceivePipeline", _frameworkOptions, _loggerFactory);
    }
}
