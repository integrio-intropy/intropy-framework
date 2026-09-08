using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Testing.Topics;

/// <summary>
/// A <see cref="SendStep{TCtx}"/> fake that captures published <see cref="CloudEvent"/>s instead of
/// publishing to a Dapr pub/sub topic. Plugged in via
/// <c>ExtractorBuilder&lt;...&gt;.WithSender(fakeTopic)</c> in place of
/// <c>WithDaprTopicPublisher</c>.
/// </summary>
/// <remarks>
/// Assert on <see cref="Count"/> and <see cref="Events"/> after running the pipeline — these are the
/// exact <see cref="CloudEvent"/> instances the real publisher would have encoded. Because this is a
/// technical step, setting <see cref="SendException"/> surfaces as a technical failure through the
/// framework's normal exception handling, matching a dead broker in production.
/// </remarks>
/// <typeparam name="TCtx">The pipeline context type; composes for any context derived from
/// <see cref="Context"/>.</typeparam>
public class FakeTopic<TCtx> : SendStep<TCtx> where TCtx : Context
{
    private readonly List<CloudEvent> _events = [];
    private readonly object _lock = new();

    private volatile Exception? _sendException;

    /// <summary>
    /// Gets the number of published events.
    /// </summary>
    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _events.Count;
            }
        }
    }

    /// <summary>
    /// Gets the published events in publication order, as a snapshot.
    /// </summary>
    public IReadOnlyList<CloudEvent> Events
    {
        get
        {
            lock (_lock)
            {
                return [.. _events];
            }
        }
    }

    /// <summary>
    /// When set, thrown by <see cref="ExecuteAsync"/> to simulate a dead broker. Clearing the
    /// property restores normal behavior. Safe to toggle between runs; not a coordination
    /// primitive for mid-run assertions.
    /// </summary>
    public Exception? SendException
    {
        get => _sendException;
        set => _sendException = value;
    }

    /// <inheritdoc/>
    /// <remarks>When <see cref="SendException"/> is set, the exception is thrown before the event
    /// is captured — a dead broker does not observe the publish.</remarks>
    public override Task<(TechnicalStepResult<CloudEvent> Result, TCtx Context)> ExecuteAsync(
        CloudEvent input, TCtx context, CancellationToken ct)
    {
        if (_sendException is not null)
        {
            throw _sendException;
        }

        lock (_lock)
        {
            _events.Add(input);
        }

        return Task.FromResult<(TechnicalStepResult<CloudEvent>, TCtx)>(
            (new TechnicalStepResult<CloudEvent>.Success(input), context));
    }
}
