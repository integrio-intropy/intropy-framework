using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Intropy.Framework.Testing.Topics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Intropy.Framework.Testing.Test.Topics;

public class FakeEnqueueStepTests
{
    private static readonly FrameworkOptions Options = new()
    {
        ComponentName = "Test",
        ServiceNamespace = "Org",
    };

    [Fact]
    public async Task ExecuteAsync_CapturesEnqueuesInOrder_WithDecodedCloudEvents()
    {
        var fake = new FakeEnqueueStep<Context>(Options);
        var first = new SourceItem("file-1.txt", "first"u8.ToArray());
        var second = new SourceItem("file-2.txt", "second"u8.ToArray());
        var context = new Context(new Dictionary<string, string> { ["sourceItemId"] = "file-1" });

        await fake.ExecuteAsync(first, context, CancellationToken.None);
        await fake.ExecuteAsync(second, context, CancellationToken.None);

        Assert.Equal(2, fake.Count);
        Assert.Same(first, fake.Captured[0].Item);
        Assert.Same(second, fake.Captured[1].Item);

        var cloudEvent = fake.Captured[0].DecodeCloudEvent();
        Assert.Equal("transactional-integration.received", cloudEvent.Type);
        Assert.Equal("first"u8.ToArray(), (byte[])cloudEvent.Data!);
    }

    [Fact]
    public async Task ExecuteAsync_ReturnsSuccessWithInput()
    {
        var fake = new FakeEnqueueStep<Context>(Options);
        var item = new SourceItem("file-1.txt", [1, 2, 3]);

        var (result, _) = await fake.ExecuteAsync(
            item, new Context(new Dictionary<string, string>()), CancellationToken.None);

        var success = Assert.IsType<TechnicalStepResult<SourceItem>.Success>(result);
        Assert.Same(item, success.Value);
    }

    [Fact]
    public async Task CapturedEnvelope_IsDefensiveCopy_NotFormatterBuffer()
    {
        var fake = new FakeEnqueueStep<Context>(Options);
        var item = new SourceItem("file-1.txt", "payload"u8.ToArray());
        var context = new Context(new Dictionary<string, string>());

        await fake.ExecuteAsync(item, context, CancellationToken.None);
        var snapshot = fake.Captured[0].Envelope.ToArray();

        // Enqueueing more items reuses the formatter; the captured bytes must not change.
        await fake.ExecuteAsync(new SourceItem("file-2.txt", "other-payload"u8.ToArray()),
            context, CancellationToken.None);

        Assert.Equal(snapshot, fake.Captured[0].Envelope);
    }

    [Fact]
    public async Task SendException_SurfacesAsTechnicalFailureThroughPublicPipeline()
    {
        var fake = new FakeEnqueueStep<Context>(Options)
        {
            SendException = new HttpRequestException("broker is dead"),
        };
        var item = new SourceItem("file-1.txt", [1]);

        // Exercised through the public pipeline surface: the framework's step wrapper converts the
        // thrown exception into a technical failure, exactly as with a dead broker in production.
        var (result, _, _) = await Pipeline
            .Start<SourceItem, Context>(item, new Context(new Dictionary<string, string>()))
            .AddStep<SourceItem, SourceItem, Context>(fake);

        Assert.IsType<StepResult<SourceItem>.TechnicalFailure>(result);
        Assert.Empty(fake.Captured);
    }

    [Fact]
    public async Task SendException_InReceivePipeline_SurfacesAsTechnicalFailure()
    {
        // The full receive-pipeline fault path: a dead broker is a technical failure, so the file
        // sweep that runs the pipeline leaves the source file for the next run.
        var fake = new FakeEnqueueStep<Context>(Options)
        {
            SendException = new HttpRequestException("broker is dead"),
        };

        var pipeline = ReceivePipelineBuilder<Context>
            .Create("TestReceivePipeline", Options, NullLoggerFactory.Instance)
            .WithEnqueuer(fake)
            .Build();

        var (result, _) = await pipeline.Execute(
            new SourceItem("file-1.txt", "content"u8.ToArray()), new Context(new Dictionary<string, string>()));

        Assert.IsType<StepResult<SourceItem>.TechnicalFailure>(result);
        Assert.Empty(fake.Captured);
    }

    [Fact]
    public async Task SendException_Cleared_RestoresNormalBehavior()
    {
        var fake = new FakeEnqueueStep<Context>(Options)
        {
            SendException = new InvalidOperationException("broker is dead"),
        };
        var item = new SourceItem("file-1.txt", [1]);
        var context = new Context(new Dictionary<string, string>());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fake.ExecuteAsync(item, ReadOnlyMemory<byte>.Empty, context, CancellationToken.None));

        fake.SendException = null;

        await fake.ExecuteAsync(item, context, CancellationToken.None);
        Assert.Equal(1, fake.Count);
    }
}
