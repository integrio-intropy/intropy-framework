using System.Text;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Hosting.Test.Messaging;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

/// <summary>
/// The send side resolving a claim-checked payload: a message carrying a pointer has its payload
/// fetched from the claim store before the pipeline runs, so no send pipeline — and no component's
/// code — ever knows the mechanism exists.
/// </summary>
public class SendRouteClaimCheckTests
{
    private readonly ISendPipeline<OrderContext> _pipeline = Substitute.For<ISendPipeline<OrderContext>>();

    private RoutingMessageHandler Processor(IFileAdapter? store)
    {
        _pipeline.Execute(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<OrderContext>(), Arg.Any<CancellationToken>())
            .Returns(call => ((StepResult<string>)new StepResult<string>.Success(""), call.Arg<OrderContext>()));
        return SendRouting.Handler(
            new MessageRouteTable([
                new RegisteredSendRoute<OrderContext>((metadata, isRetry) => new OrderContext(metadata, isRetry))
            ]),
            services =>
            {
                services.AddSingleton(_pipeline);
                if (store is not null)
                    services.AddKeyedSingleton(ClaimStoreServiceCollectionExtensions.ClaimStoreKey, store);
            });
    }

    private static IncomingMessage Message(string? dataRef) =>
        IncomingMessage.From(MessageConsumerTests.Request("msg-1", extensions: dataRef is null
            ? null
            : new Dictionary<string, string> { [CloudEventExtensionKeys.DataRef] = dataRef }));

    private byte[] DataGivenToThePipeline() =>
        _pipeline.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(ISendPipeline<OrderContext>.Execute))
            .Select(c => ((ReadOnlyMemory<byte>)c.GetArguments()[0]!).ToArray())
            .Single();

    [Fact]
    public async Task HandleAsync_FetchesTheClaimedPayload_AndHandsItToThePipeline()
    {
        const string key = "orders/2026/03/07/msg-1";
        var adapter = new InMemoryFileAdapter().AddFile(key, "{\"orderId\":\"ORD-1\"}");

        var handled = await Processor(adapter)
            .HandleAsync(Message(key), null, CancellationToken.None, CancellationToken.None);

        Assert.Equal(MessageOutcome.Processed, handled.Outcome.Outcome);
        Assert.Equal("{\"orderId\":\"ORD-1\"}", Encoding.UTF8.GetString(DataGivenToThePipeline()));
    }

    [Fact]
    public async Task HandleAsync_UsesTheMessagesOwnData_WhenItCarriesNoPointer()
    {
        var adapter = new InMemoryFileAdapter();

        var handled = await Processor(adapter)
            .HandleAsync(Message(dataRef: null), null, CancellationToken.None, CancellationToken.None);

        Assert.Equal(MessageOutcome.Processed, handled.Outcome.Outcome);
        Assert.Equal("{}", Encoding.UTF8.GetString(DataGivenToThePipeline()));
    }

    [Fact]
    public async Task HandleAsync_FailsForRedelivery_WhenTheClaimedPayloadIsMissing()
    {
        // Missing or unreachable is a technical failure: the broker redelivers and eventually
        // dead-letters it. Never a business failure, which would resolve the message as handled.
        var handled = await Processor(new InMemoryFileAdapter())
            .HandleAsync(Message("orders/2026/03/07/gone"), null, CancellationToken.None, CancellationToken.None);

        Assert.Equal(MessageOutcome.Failed, handled.Outcome.Outcome);
        Assert.Equal(typeof(FileNotFoundException).FullName, handled.Outcome.ErrorType);
    }

    [Fact]
    public async Task HandleAsync_SaysSo_WhenAPointerArrivesWithNoClaimStoreConfigured()
    {
        // The shape of a mis-ordered rollout: a publisher claim-checking before its consumers can
        // resolve. The error names that, rather than surfacing later as an empty payload.
        var handled = await Processor(store: null)
            .HandleAsync(Message("orders/2026/03/07/msg-1"), null, CancellationToken.None, CancellationToken.None);

        Assert.Equal(MessageOutcome.Failed, handled.Outcome.Outcome);
        Assert.Equal(typeof(InvalidOperationException).FullName, handled.Outcome.ErrorType);
        Assert.Contains("no claim store is registered", handled.Outcome.Description, StringComparison.Ordinal);
    }

    public sealed record OrderContext(Dictionary<string, string> Metadata, bool IsRetry) : Context(Metadata, IsRetry);
}
