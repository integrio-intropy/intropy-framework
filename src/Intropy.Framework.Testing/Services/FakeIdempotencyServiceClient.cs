using Intropy.Contracts.IdempotencyService;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Testing.Services;

/// <summary>
/// A stateful <see cref="IIdempotencyServiceClient"/> fake for component integration tests.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="GetStatusAsync"/> dequeues statuses queued via <see cref="QueueStatus"/> while the
/// queue is non-empty, then falls back to the sticky <see cref="NextStatus"/> — covering
/// "first check proceeds, second check ignores" sequences with one fake.
/// </para>
/// <para>
/// <see cref="CommitAsync"/> records into <see cref="Committed"/> and an internal store, so
/// <see cref="GetInfoAsync"/> returns the committed <see cref="MessageInfo"/> for a
/// component/id pair that has been committed, and null otherwise.
/// </para>
/// <para>
/// Set <see cref="StatusException"/> to simulate the service being down. The typed exception is
/// thrown before the call is recorded — a down service does not observe the request. All state is
/// guarded by a lock; assertion members return snapshots.
/// </para>
/// </remarks>
public sealed class FakeIdempotencyServiceClient : IIdempotencyServiceClient
{
    private readonly Queue<StatusResponse> _queuedStatuses = new();
    private readonly List<MessageInfo> _statusChecks = [];
    private readonly List<MessageInfo> _committed = [];
    private readonly Dictionary<(string Component, string Id), MessageInfo> _store = new();
    private readonly object _lock = new();

    /// <summary>
    /// The sticky status returned by <see cref="GetStatusAsync"/> when the queue set up by
    /// <see cref="QueueStatus"/> is empty. Defaults to
    /// <c>Proceed</c> / <c>NoPreviousData</c>.
    /// </summary>
    public StatusResponse NextStatus { get; set; } = new(Action.Proceed, Reason.NoPreviousData);

    /// <summary>
    /// When set, thrown by <see cref="GetStatusAsync"/> to simulate the idempotency service being
    /// down. The typed exception matches the real client's failure mode. Clearing the property
    /// restores normal behavior.
    /// </summary>
    public IdempotencyServiceException? StatusException { get; set; }

    /// <summary>
    /// Gets every <see cref="MessageInfo"/> passed to <see cref="GetStatusAsync"/>, in call order,
    /// as a snapshot.
    /// </summary>
    public IReadOnlyList<MessageInfo> StatusChecks
    {
        get
        {
            lock (_lock)
            {
                return [.. _statusChecks];
            }
        }
    }

    /// <summary>
    /// Gets every <see cref="MessageInfo"/> passed to <see cref="CommitAsync"/>, in call order, as a
    /// snapshot.
    /// </summary>
    public IReadOnlyList<MessageInfo> Committed
    {
        get
        {
            lock (_lock)
            {
                return [.. _committed];
            }
        }
    }

    /// <summary>
    /// Queues statuses to be returned by <see cref="GetStatusAsync"/> in order. When the queue runs
    /// dry, <see cref="GetStatusAsync"/> falls back to <see cref="NextStatus"/>.
    /// </summary>
    /// <param name="statuses">The statuses to dequeue, in order.</param>
    public void QueueStatus(params StatusResponse[] statuses)
    {
        ArgumentNullException.ThrowIfNull(statuses);

        lock (_lock)
        {
            foreach (var status in statuses)
            {
                _queuedStatuses.Enqueue(status);
            }
        }
    }

    /// <inheritdoc/>
    public Task<StatusResponse> GetStatusAsync(MessageInfo messageInfo, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messageInfo);

        if (StatusException is not null)
        {
            throw StatusException;
        }

        lock (_lock)
        {
            _statusChecks.Add(messageInfo);
            var status = _queuedStatuses.Count > 0 ? _queuedStatuses.Dequeue() : NextStatus;
            return Task.FromResult(status);
        }
    }

    /// <inheritdoc/>
    public Task CommitAsync(MessageInfo messageInfo, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messageInfo);

        lock (_lock)
        {
            _committed.Add(messageInfo);
            _store[(messageInfo.Component, messageInfo.Id)] = messageInfo;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    /// <remarks>Returns the committed <see cref="MessageInfo"/> for the component/id pair, or null
    /// when nothing has been committed for it.</remarks>
    public Task<MessageInfo?> GetInfoAsync(string component, string id, CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            return Task.FromResult(_store.TryGetValue((component, id), out var info) ? info : null);
        }
    }
}
