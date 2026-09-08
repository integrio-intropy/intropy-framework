using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
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
    public async Task SendException_InReceivePipeline_SkipsCompleter_LeavingSourceFile()
    {
        // The full receive-pipeline fault path: a dead broker is a technical failure, so the
        // completer never runs and the source file is left for the next run.
        var completerRan = false;
        var fake = new FakeEnqueueStep<Context>(Options)
        {
            SendException = new HttpRequestException("broker is dead"),
        };

        var pipeline = ReceivePipelineBuilder<Context>
            .Create("TestReceivePipeline", Options, NullLoggerFactory.Instance)
            .WithReceiver(new StaticReceiver())
            .WithEnqueuer(fake)
            .WithCompleter(new TrackingCompleter(() => completerRan = true))
            .Build();

        var (result, _) = await pipeline.Execute(
            new SourceItemInfo("file-1.txt"), new Context(new Dictionary<string, string>()));

        Assert.IsType<StepResult<SourceItem>.TechnicalFailure>(result);
        Assert.False(completerRan);
        Assert.Empty(fake.Captured);
    }

    [Fact]
    public async Task CompleteFailure_EnqueueAlreadyCaptured_ItemIdentitySurvives()
    {
        // Enqueue succeeds, delete fails: the duplicate is on the queue (capture survives) while the
        // pipeline result is a business failure — send-side idempotency must absorb the redelivery.
        var fake = new FakeEnqueueStep<Context>(Options);

        var pipeline = ReceivePipelineBuilder<Context>
            .Create("TestReceivePipeline", Options, NullLoggerFactory.Instance)
            .WithReceiver(new StaticReceiver())
            .WithEnqueuer(fake)
            .WithCompleter(new FailingCompleter())
            .Build();

        var (result, _) = await pipeline.Execute(
            new SourceItemInfo("file-1.txt"), new Context(new Dictionary<string, string>()));

        Assert.IsType<StepResult<SourceItem>.BusinessFailure>(result);
        var captured = Assert.Single(fake.Captured);
        Assert.Equal("file-1.txt", captured.Item.Id);
        Assert.Equal("transactional-integration.received", captured.DecodeCloudEvent().Type);
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

    private sealed class StaticReceiver : ReceiveStep<Context>
    {
        public override Task<(BusinessStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
            SourceItemInfo input, Context context, CancellationToken ct)
        {
            var item = new SourceItem(input.Id, "content"u8.ToArray());
            return Task.FromResult<(BusinessStepResult<SourceItem>, Context)>(
                (new BusinessStepResult<SourceItem>.Success(item), context));
        }
    }

    private sealed class TrackingCompleter(Action onRun) : CompleteStep<Context>
    {
        public override Task<(BusinessStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
            SourceItem input, Context context, CancellationToken ct)
        {
            onRun();
            return Task.FromResult<(BusinessStepResult<SourceItem>, Context)>(
                (new BusinessStepResult<SourceItem>.Success(input), context));
        }
    }

    private sealed class FailingCompleter : CompleteStep<Context>
    {
        public override Task<(BusinessStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
            SourceItem input, Context context, CancellationToken ct)
        {
            throw new IOException($"Failed to delete: {input.Id}");
        }
    }
}
