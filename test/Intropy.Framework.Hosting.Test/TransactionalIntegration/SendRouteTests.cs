using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Hosting.Test.Messaging;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Microsoft.Extensions.DependencyInjection;
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

    private void PipelineReturns(StepResult<byte[]> result) =>
        _pipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<OrderContext>(), Arg.Any<CancellationToken>())
            .Returns(call => (result, call.Arg<OrderContext>()));

    [Fact]
    public async Task HandleAsync_RebuildsTheIntegrationsContextFromThePropagatedMetadata()
    {
        OrderContext? received = null;
        _pipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Do<OrderContext>(c => received = c),
                Arg.Any<CancellationToken>())
            .Returns(call => ((StepResult<byte[]>)new StepResult<byte[]>.Success([]), call.Arg<OrderContext>()));

        await Processor().HandleAsync(Message("msg-1", new Dictionary<string, string>
        {
            ["metadata"] =
                "{\"file_name\":\"orders.csv\",\"source_port\":\"orders-in\",\"received_at\":\"2026-10-14T06:30:00.0000000+00:00\"}",
            ["retrycount"] = "1"
        }), null, CancellationToken.None, CancellationToken.None);

        Assert.NotNull(received);
        Assert.Equal("orders.csv", received.Metadata[SourceContextKeys.FileName]);
        Assert.Equal("orders-in", received.Metadata[SourceContextKeys.SourcePort]);
        // Restored from the message, so every redelivery of it sees the same pickup time.
        Assert.Equal("2026-10-14T06:30:00.0000000+00:00", received.Metadata[SourceContextKeys.ReceivedAt]);
        Assert.Equal("msg-1", received.Metadata[ContextKeys.MessageId]);
        Assert.True(received.IsRetry);
    }

    [Fact]
    public async Task HandleAsync_GivesEachMessageItsOwnContext_WithoutPropagatedMetadata()
    {
        var received = new List<OrderContext>();
        _pipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Do<OrderContext>(received.Add),
                Arg.Any<CancellationToken>())
            .Returns(call => ((StepResult<byte[]>)new StepResult<byte[]>.Success([]), call.Arg<OrderContext>()));

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
            .Returns(call => ((StepResult<byte[]>)new StepResult<byte[]>.Success([]), call.Arg<OrderContext>()));

        await Processor().HandleAsync(Message(), null, CancellationToken.None, CancellationToken.None);

        Assert.Equal("{}", System.Text.Encoding.UTF8.GetString(data!.Value.Span));
    }

    public static TheoryData<StepResult<byte[]>, string, string?> ResultsAndOutcomes => new()
    {
        { new StepResult<byte[]>.Success([]), nameof(MessageOutcome.Processed), null },
        { new StepResult<byte[]>.Cancelled(), nameof(MessageOutcome.Skipped), null },
        { new StepResult<byte[]>.TechnicalFailure(new TechnicalFailure("send failed")), nameof(MessageOutcome.Failed), "technical_failure" },
        // A retriable business failure — the default, and what an unreachable destination produces
        // — is left for redelivery: its incident has been routed, but the payload is intact and a
        // later attempt may deliver it.
        { new StepResult<byte[]>.BusinessFailure(new BusinessIncidentData { Description = "unreachable", Context = [] }), nameof(MessageOutcome.Failed), "business_failure" },
        // A terminal one only reaches this method when no incident router is configured: nothing
        // reported the problem, so acknowledging it would lose it silently. With a router it has
        // already been turned into a success, and never arrives here as a failure.
        { new StepResult<byte[]>.BusinessFailure(new BusinessIncidentData { Description = "rejected", Context = [] }, Retriable: false), nameof(MessageOutcome.Failed), "business_failure" },
        // Aborted without being interrupted: for example the processing time limit.
        { new StepResult<byte[]>.Aborted(), nameof(MessageOutcome.Failed), "aborted" }
    };

    [Theory]
    [MemberData(nameof(ResultsAndOutcomes))]
    public async Task HandleAsync_MapsThePipelinesResultToTheMessagesOutcome(StepResult<byte[]> result,
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
        PipelineReturns(new StepResult<byte[]>.Aborted());
        using var interrupt = new CancellationTokenSource();
        await interrupt.CancelAsync();

        var handled = await Processor().HandleAsync(Message(), null, interrupt.Token, interrupt.Token);

        Assert.Equal(MessageOutcome.Interrupted, handled.Outcome.Outcome);
        Assert.Null(handled.Outcome.ErrorType);
    }

    [Fact]
    public async Task HandleAsync_WithoutRoutes_ReportsNoRouteName()
    {
        PipelineReturns(new StepResult<byte[]>.Success([]));

        var handled = await Processor().HandleAsync(Message(), null, CancellationToken.None, CancellationToken.None);

        Assert.Null(handled.Route);
    }

    [Theory]
    [InlineData(null, "No send route handles source port '(none)': it names no port")]
    [InlineData("", "No send route handles source port '(none)': it names no port")]
    [InlineData("orders-in", "No send route handles source port 'orders-in'")]
    public void UnroutedSend_NamesTheStampedPort_OrRendersNoneWhenTheStampIsAbsent(string? port, string expected)
    {
        var extensions = port is null
            ? new Dictionary<string, string>()
            : new Dictionary<string, string> { [CloudEventExtensionKeys.SourcePort] = port };

        Assert.Equal(expected,
            TransactionalIntegrationServiceCollectionExtensions.DescribeUnroutedSend(Message(extensions: extensions)));
    }

    [Fact]
    public async Task HandleAsync_AMessageWithoutAStamp_IsUnrouted_NamesNoPortRatherThanAnEmptyOne()
    {
        // The message was published without the stamp: it names no port, not a port with an
        // empty name.
        var handler = SendRouting.Handler(
            new MessageRouteTable([new NeverMatchingRoute()]),
            describeUnrouted: TransactionalIntegrationServiceCollectionExtensions.DescribeUnroutedSend);

        var handled = await handler.HandleAsync(Message(), null, CancellationToken.None, CancellationToken.None);

        Assert.Equal(MessageOutcome.Unrouted, handled.Outcome.Outcome);
        Assert.Equal("No send route handles source port '(none)': it names no port", handled.Outcome.Description);
    }

    [Theory]
    [InlineData("orders-in", true)]
    [InlineData("orders-no", true)]
    [InlineData("invoices-in", false)]
    [InlineData("ORDERS-IN", false)] // ports match exactly
    [InlineData(null, false)] // a message naming no port never matches
    public void DeclaredRoute_MatchesOnTheRecordedPort(string? port, bool matches)
    {
        var route = Declared("orders-in", "orders-no");
        var extensions = new Dictionary<string, string>();
        if (port is not null)
            extensions[CloudEventExtensionKeys.SourcePort] = port;

        Assert.Equal(matches, route.Matches(Message("msg-1", extensions)));
    }

    [Fact]
    public void DeclaredRoute_IgnoresTheEditableMetadata()
    {
        // A receive step rewrote source_port; the framework-recorded extension decides.
        var route = Declared("invoices-in");

        Assert.False(route.Matches(Message("msg-1", new Dictionary<string, string>
        {
            [CloudEventExtensionKeys.SourcePort] = "orders-in",
            ["metadata"] = "{\"source_port\":\"invoices-in\"}"
        })));
    }

    [Fact]
    public void DeclaredRoute_IsNamedAfterItsPorts()
    {
        Assert.Equal("orders-in+orders-no", Declared("orders-in", "orders-no").Name);
        Assert.Null(new DeclaredSendRoute<string, string, OrderContext>(null, (send, _) => send,
            (metadata, isRetry) => new OrderContext(metadata, isRetry)).Name);
    }

    private static DeclaredSendRoute<string, string, OrderContext> Declared(params string[] ports) =>
        new(new FileMatch(ports), (send, _) => send,
            (metadata, isRetry) => new OrderContext(metadata, isRetry));

    /// <summary>A route whose match is never true, so every message reaches the unrouted path.</summary>
    private sealed class NeverMatchingRoute : IMessageRoute
    {
        public string? Name => "never";

        public bool IsCatchAll => false;

        public bool Matches(IncomingMessage message) => false;

        public void Verify(IServiceProvider scope, string componentName) { }

        public Task<PipelineOutcome> ExecuteAsync(IncomingMessage message, IServiceScopeFactory scopes,
            string componentName, CancellationToken interrupt, CancellationToken cancellationToken) =>
            Task.FromResult(new PipelineOutcome(MessageOutcome.Processed));
    }

    public sealed record OrderContext(Dictionary<string, string> Metadata, bool IsRetry) : Context(Metadata, IsRetry);
}
