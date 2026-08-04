using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Intropy.Framework.Testing.Topics;

namespace Intropy.Framework.Testing.Test.Topics;

public class FakeTopicTests
{
    private static readonly Uri Source = new("urn:test");

    [Fact]
    public async Task ExecuteAsync_CapturesEventsInPublicationOrder()
    {
        var topic = new FakeTopic<Context>();
        var first = new CloudEvent { Id = "1", Source = Source, Type = "test" };
        var second = new CloudEvent { Id = "2", Source = Source, Type = "test" };
        var context = new Context(new Dictionary<string, string>());

        await topic.ExecuteAsync(first, context, CancellationToken.None);
        await topic.ExecuteAsync(second, context, CancellationToken.None);

        Assert.Equal(2, topic.Count);
        Assert.Same(first, topic.Events[0]);
        Assert.Same(second, topic.Events[1]);
    }

    [Fact]
    public async Task ExecuteAsync_ReturnsSuccessWithInput()
    {
        var topic = new FakeTopic<Context>();
        var cloudEvent = new CloudEvent { Id = "1", Source = Source, Type = "test" };

        var (result, _) = await topic.ExecuteAsync(
            cloudEvent, new Context(new Dictionary<string, string>()), CancellationToken.None);

        var success = Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(result);
        Assert.Same(cloudEvent, success.Value);
    }

    [Fact]
    public async Task SendException_SurfacesAsTechnicalFailureThroughPublicPipeline()
    {
        var topic = new FakeTopic<Context>
        {
            SendException = new HttpRequestException("broker is dead"),
        };
        var cloudEvent = new CloudEvent { Id = "1", Source = Source, Type = "test" };

        // Exercised through the public pipeline surface: the framework's step wrapper converts the
        // thrown exception into a technical failure, exactly as with a dead broker in production.
        var (result, _, _) = await Pipeline
            .Start<CloudEvent, Context>(cloudEvent, new Context(new Dictionary<string, string>()))
            .AddStep<CloudEvent, CloudEvent, Context>(topic);

        Assert.IsType<StepResult<CloudEvent>.TechnicalFailure>(result);
        Assert.Empty(topic.Events);
    }

    [Fact]
    public async Task SendException_Cleared_RestoresNormalBehavior()
    {
        var topic = new FakeTopic<Context>
        {
            SendException = new InvalidOperationException("broker is dead"),
        };
        var cloudEvent = new CloudEvent { Id = "1", Source = Source, Type = "test" };
        var context = new Context(new Dictionary<string, string>());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => topic.ExecuteAsync(cloudEvent, context, CancellationToken.None));

        topic.SendException = null;

        await topic.ExecuteAsync(cloudEvent, context, CancellationToken.None);
        Assert.Equal(1, topic.Count);
    }
}
