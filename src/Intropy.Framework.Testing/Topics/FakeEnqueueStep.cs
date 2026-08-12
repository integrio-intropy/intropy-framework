using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Testing.Topics;

/// <summary>
/// An <see cref="EnqueueStep{TCtx}"/> fake that captures the items and their encoded
/// structured-mode CloudEvents envelopes instead of publishing to a message queue. Plugged in via
/// <c>ReceivePipelineBuilder&lt;TCtx&gt;.WithEnqueuer(fakeEnqueue)</c> in place of the component's
/// Dapr enqueuer.
/// </summary>
/// <remarks>
/// <para>
/// The base class's sealed 3-arg <c>ExecuteAsync</c> builds the envelope exactly as production does
/// (context metadata, W3C trace propagation, JSON structured-mode encoding) and hands it to this
/// override, so <see cref="CapturedEnqueue.Envelope"/> is the exact payload the real broker would
/// have received. Decode it via <see cref="CapturedEnqueue.DecodeCloudEvent"/>.
/// </para>
/// <para>
/// Because this is a technical step, setting <see cref="SendException"/> surfaces as a technical
/// failure through the framework's normal exception handling, matching a dead broker in production.
/// And because the complete step follows the enqueue step, a thrown exception also leaves the source
/// file undeleted — the same fault semantics as a real broker outage.
/// </para>
/// <para>
/// Named after the base class it fakes rather than after <c>FakeTopic&lt;TCtx&gt;</c> — "SendStep"
/// already names three unrelated steps in the framework, so the fake names the actual seam.
/// </para>
/// </remarks>
/// <typeparam name="TCtx">The pipeline context type; composes for any context derived from
/// <see cref="Context"/>.</typeparam>
public class FakeEnqueueStep<TCtx>(FrameworkOptions options) : EnqueueStep<TCtx>(options)
    where TCtx : Context
{
    private readonly List<CapturedEnqueue> _captured = [];
    private readonly object _lock = new();

    private volatile Exception? _sendException;

    /// <summary>
    /// Gets the captured enqueues in enqueue order, as a snapshot.
    /// </summary>
    public IReadOnlyList<CapturedEnqueue> Captured
    {
        get
        {
            lock (_lock)
            {
                return [.. _captured];
            }
        }
    }

    /// <summary>
    /// Gets the number of captured enqueues.
    /// </summary>
    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _captured.Count;
            }
        }
    }

    /// <summary>
    /// When set, thrown by <see cref="ExecuteAsync(SourceItem, ReadOnlyMemory{byte}, TCtx, CancellationToken)"/>
    /// to simulate a dead broker. Clearing the property restores normal behavior. Safe to toggle
    /// between runs; not a coordination primitive for mid-run assertions.
    /// </summary>
    public Exception? SendException
    {
        get => _sendException;
        set => _sendException = value;
    }

    /// <inheritdoc/>
    /// <remarks>When <see cref="SendException"/> is set, the exception is thrown before the enqueue
    /// is captured — a dead broker does not observe the publish.</remarks>
    public override Task<(TechnicalStepResult<SourceItem> Result, TCtx Context)> ExecuteAsync(
        SourceItem input, ReadOnlyMemory<byte> cloudEvent, TCtx context, CancellationToken ct)
    {
        if (_sendException is not null)
        {
            throw _sendException;
        }

        lock (_lock)
        {
            // Defensive copy (parity with PublishedMessage.Data): the formatter's buffer may be
            // recycled, so capturing the ReadOnlyMemory directly would be a latent aliasing bug.
            _captured.Add(new CapturedEnqueue(input, cloudEvent.ToArray()));
        }

        return Task.FromResult<(TechnicalStepResult<SourceItem>, TCtx)>(
            (new TechnicalStepResult<SourceItem>.Success(input), context));
    }
}
