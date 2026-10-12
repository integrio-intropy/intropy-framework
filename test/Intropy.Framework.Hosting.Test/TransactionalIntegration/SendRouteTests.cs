using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Hosting.Test.Messaging;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

/// <summary>
/// A Transactional Integration's send routes, through the routing handler the send side runs: a
/// route rebuilds the integration's context from what the receive side propagated, runs the
/// message's data through its send pipeline, and maps the result to the message's outcome. A
/// declared route matches on the source port and file name the framework recorded on the message,
/// never on the editable metadata.
/// </summary>
public class SendRouteTests
{
    private readonly ISendPipeline<OrderContext> _pipeline = Substitute.For<ISendPipeline<OrderContext>>();

    private RoutingMessageHandler Processor() =>
        SendRouting.Handler(_pipeline, (metadata, isRetry) => new OrderContext(metadata, isRetry));

    private static IncomingMessage Message(string id = "msg-1", IDictionary<string, string>? extensions = null) =>
        IncomingMessage.From(MessageConsumerTests.Request(id, extensions: extensions));

    private void PipelineReturns(StepResult<string> result) =>
        _pipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<OrderContext>(), Arg.Any<CancellationToken>())
            .Returns(call => (result, call.Arg<OrderContext>()));

    [Fact]
    public async Task HandleAsync_RebuildsTheIntegrationsContextFromThePropagatedMetadata()
    {
        OrderContext? received = null;
        _pipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Do<OrderContext>(c => received = c),
                Arg.Any<CancellationToken>())
            .Returns(call => ((StepResult<string>)new StepResult<string>.Success(""), call.Arg<OrderContext>()));

        await Processor().HandleAsync(Message("msg-1", new Dictionary<string, string>
        {
            ["metadata"] = "{\"file_name\":\"orders.csv\",\"source_port\":\"orders-in\"}",
            ["retrycount"] = "1"
        }), null, CancellationToken.None, CancellationToken.None);

        Assert.NotNull(received);
        Assert.Equal("orders.csv", received.Metadata[SourceContextKeys.FileName]);
        Assert.Equal("orders-in", received.Metadata[SourceContextKeys.SourcePort]);
        Assert.Equal("msg-1", received.Metadata[ContextKeys.MessageId]);
        Assert.True(received.IsRetry);
    }

    [Fact]
    public async Task HandleAsync_GivesEachMessageItsOwnContext_WithoutPropagatedMetadata()
    {
        var received = new List<OrderContext>();
        _pipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Do<OrderContext>(received.Add),
                Arg.Any<CancellationToken>())
            .Returns(call => ((StepResult<string>)new StepResult<string>.Success(""), call.Arg<OrderContext>()));

        await Processor().HandleAsync(Message("msg-1"), null, CancellationToken.None, CancellationToken.None);
        await Processor().HandleAsync(Message("msg-2"), null, CancellationToken.None, CancellationToken.None);

        Assert.NotSame(received[0].Metadata, received[1].Metadata);
        Assert.Equal(["msg-1", "msg-2"], received.Select(c => c.Metadata[ContextKeys.MessageId]));
        Assert.All(received, c => Assert.False(c.IsRetry));
    }

    [Fact]
    public async Task HandleAsync_PassesTheMessagesDataToThePipeline()
    {
        ReadOnlyMemory<byte>? data = null;
        _pipeline.Execute(Arg.Do<ReadOnlyMemory<byte>>(d => data = d), Arg.Any<OrderContext>(),
                Arg.Any<CancellationToken>())
            .Returns(call => ((StepResult<string>)new StepResult<string>.Success(""), call.Arg<OrderContext>()));

        await Processor().HandleAsync(Message(), null, CancellationToken.None, CancellationToken.None);

        Assert.Equal("{}", System.Text.Encoding.UTF8.GetString(data!.Value.Span));
    }

    public static TheoryData<StepResult<string>, string, string?> ResultsAndOutcomes => new()
    {
        { new StepResult<string>.Success(""), nameof(MessageOutcome.Processed), null },
        { new StepResult<string>.Cancelled(), nameof(MessageOutcome.Skipped), null },
        { new StepResult<string>.TechnicalFailure(new TechnicalFailure("send failed")), nameof(MessageOutcome.Failed), "technical_failure" },
        // A Transactional Integration leaves a business failure for redelivery too.
        { new StepResult<string>.BusinessFailure(new BusinessIncidentData { Description = "rejected", Context = [] }), nameof(MessageOutcome.Failed), "business_failure" },
        // Aborted without being interrupted: for example the processing time limit.
        { new StepResult<string>.Aborted(), nameof(MessageOutcome.Failed), "aborted" }
    };

    [Theory]
    [MemberData(nameof(ResultsAndOutcomes))]
    public async Task HandleAsync_MapsThePipelinesResultToTheMessagesOutcome(StepResult<string> result,
        string outcome, string? errorType)
    {
        PipelineReturns(result);

        var handled = await Processor().HandleAsync(Message(), null, CancellationToken.None, CancellationToken.None);

        Assert.Equal(Enum.Parse<MessageOutcome>(outcome), handled.Outcome.Outcome);
        Assert.Equal(errorType, handled.Outcome.ErrorType);
    }

    [Fact]
    public async Task HandleAsync_AnAbortedPipeline_IsAnInterruption_WhenTheConsumerInterruptedIt()
    {
        PipelineReturns(new StepResult<string>.Aborted());
        using var interrupt = new CancellationTokenSource();
        await interrupt.CancelAsync();

        var handled = await Processor().HandleAsync(Message(), null, interrupt.Token, interrupt.Token);

        Assert.Equal(MessageOutcome.Interrupted, handled.Outcome.Outcome);
        Assert.Null(handled.Outcome.ErrorType);
    }

    [Fact]
    public async Task HandleAsync_WithoutRoutes_ReportsNoRouteName()
    {
        PipelineReturns(new StepResult<string>.Success(""));

        var handled = await Processor().HandleAsync(Message(), null, CancellationToken.None, CancellationToken.None);

        Assert.Null(handled.Route);
    }

    [Theory]
    [InlineData("orders-in", "order-1.csv", true)]
    [InlineData("orders-in", "ORDER-1.CSV", true)] // file name patterns ignore case
    [InlineData("orders-in", "order-1.xml", false)]
    [InlineData("invoices-in", "order-1.csv", false)]
    [InlineData(null, "order-1.csv", false)] // an unknown port never matches a port criterion
    [InlineData("orders-in", null, false)]
    public void DeclaredRoute_MatchesOnTheRecordedPortAndFileName(string? port, string? fileName, bool matches)
    {
        var route = Declared(m => m.Port("orders-in", "orders-no").FileName("*.csv"));
        var extensions = new Dictionary<string, string>();
        if (port is not null)
            extensions[CloudEventExtensionKeys.SourcePort] = port;
        if (fileName is not null)
            extensions[CloudEventExtensionKeys.FileName] = fileName;

        Assert.Equal(matches, route.Matches(Message("msg-1", extensions)));
    }

    [Fact]
    public void DeclaredRoute_IgnoresTheEditableMetadata()
    {
        // A receive step rewrote source_port and file_name; the framework-recorded extensions decide.
        var route = Declared(m => m.Port("invoices-in"));

        Assert.False(route.Matches(Message("msg-1", new Dictionary<string, string>
        {
            [CloudEventExtensionKeys.SourcePort] = "orders-in",
            [CloudEventExtensionKeys.FileName] = "order-1.csv",
            ["metadata"] = "{\"source_port\":\"invoices-in\"}"
        })));
    }

    [Fact]
    public void DeclaredRoute_IsNamedAfterItsMatch()
    {
        Assert.Equal("orders-in+orders-no(*.csv|*.txt)",
            Declared(m => m.Port("orders-in", "orders-no").FileName("*.csv", "*.txt")).Name);
        Assert.Equal("*(*.xml)", Declared(m => m.FileName("*.xml")).Name);
        Assert.Null(new DeclaredSendRoute<string, string, OrderContext>(null, (send, _) => send,
            (metadata, isRetry) => new OrderContext(metadata, isRetry)).Name);
    }

    private static DeclaredSendRoute<string, string, OrderContext> Declared(
        Func<TransactionalIntegrationMatch, TransactionalIntegrationMatch> match) =>
        new(match(new TransactionalIntegrationMatch()), (send, _) => send,
            (metadata, isRetry) => new OrderContext(metadata, isRetry));

    public sealed record OrderContext(Dictionary<string, string> Metadata, bool IsRetry) : Context(Metadata, IsRetry);
}
