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
/// The Transactional Integration's message handler: rebuilds the integration's context from what
/// the receive side propagated, runs the message's data through the send pipeline, and maps the
/// result to the message's outcome.
/// </summary>
public class MessageProcessorTests
{
    private readonly ISendPipeline<OrderContext> _pipeline = Substitute.For<ISendPipeline<OrderContext>>();

    private MessageProcessor<OrderContext> Processor() =>
        new(_pipeline, (metadata, isRetry) => new OrderContext(metadata, isRetry), "test-integration",
            NullLogger<MessageProcessor<OrderContext>>.Instance);

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
            ["metadata"] = "{\"file_name\":\"orders.csv\"}",
            ["retrycount"] = "1"
        }), null, CancellationToken.None, CancellationToken.None);

        Assert.NotNull(received);
        Assert.Equal("orders.csv", received.Metadata[SourceContextKeys.FileName]);
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

    public sealed record OrderContext(Dictionary<string, string> Metadata, bool IsRetry) : Context(Metadata, IsRetry);
}
