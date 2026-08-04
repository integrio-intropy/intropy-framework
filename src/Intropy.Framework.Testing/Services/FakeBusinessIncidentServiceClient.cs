using Intropy.Contracts.BusinessIncidentService;

namespace Intropy.Framework.Testing.Services;

/// <summary>
/// A stateful <see cref="IBusinessIncidentServiceClient"/> fake for component integration tests.
/// </summary>
/// <remarks>
/// <para>
/// Every <see cref="Trigger"/> call is recorded in <see cref="Incidents"/>; assert on incident
/// contents directly, e.g. <c>fake.Incidents.Single().Data.Description</c>. Every
/// <see cref="Resolve"/> call is appended to <see cref="Resolved"/> and marks the matching recorded
/// incident resolved — covering the retry path where the framework resolves a previously triggered
/// incident on success. An unmatched resolve is still recorded and otherwise no-ops (never throws).
/// </para>
/// <para>
/// Set <see cref="TriggerException"/> to simulate the service being down. The typed exception
/// matters: the framework's incident router has a dedicated catch for
/// <see cref="BusinessIncidentServiceException"/>; any other exception type would take a different
/// code path than production. The exception is thrown before the call is recorded.
/// </para>
/// <para>
/// <see cref="GetById"/>, <see cref="List"/>, <see cref="GetEvents"/>, and
/// <see cref="ResolveManual"/> are served from the recorded state. All state is guarded by a lock;
/// assertion members return snapshots.
/// </para>
/// </remarks>
public sealed class FakeBusinessIncidentServiceClient : IBusinessIncidentServiceClient
{
    private const string StatusTriggered = "Triggered";
    private const string StatusResolved = "Resolved";
    private const string ResolvedBySystem = "system";

    private readonly List<RecordedIncident> _incidents = [];
    private readonly List<ResolvedIncident> _resolved = [];
    private readonly List<Projection> _projections = [];
    private readonly object _lock = new();

    /// <summary>
    /// When set, thrown by <see cref="Trigger"/> to simulate the business incident service being
    /// down. Clearing the property restores normal behavior.
    /// </summary>
    public BusinessIncidentServiceException? TriggerException { get; set; }

    /// <summary>
    /// Gets every triggered incident, in call order, as a snapshot.
    /// </summary>
    public IReadOnlyList<RecordedIncident> Incidents
    {
        get
        {
            lock (_lock)
            {
                return [.. _incidents];
            }
        }
    }

    /// <summary>
    /// Gets every resolve call, in call order, as a snapshot — including resolves that matched no
    /// triggered incident.
    /// </summary>
    public IReadOnlyList<ResolvedIncident> Resolved
    {
        get
        {
            lock (_lock)
            {
                return [.. _resolved];
            }
        }
    }

    /// <inheritdoc/>
    public Task Trigger(Uri source, string subject, string id, BusinessIncidentData data, string? batchId)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(data);

        if (TriggerException is not null)
        {
            throw TriggerException;
        }

        lock (_lock)
        {
            _incidents.Add(new RecordedIncident(source, subject, id, data, batchId));
            _projections.Add(new Projection
            {
                Response = new IncidentResponse
                {
                    Id = Guid.NewGuid(),
                    Source = source.ToString(),
                    CeId = id,
                    Subject = subject,
                    Description = data.Description,
                    Status = StatusTriggered,
                    BatchId = batchId,
                    Context = data.Context,
                    TriggeredAt = DateTimeOffset.UtcNow,
                },
            });
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    /// <remarks>Always appends to <see cref="Resolved"/> and marks the matching incident resolved.
    /// An unmatched resolve is recorded and otherwise no-ops (never throws).</remarks>
    public Task Resolve(Uri source, string subject, string id, string? batchId)
    {
        ArgumentNullException.ThrowIfNull(source);

        lock (_lock)
        {
            _resolved.Add(new ResolvedIncident(source, subject, id, batchId));

            var projection = _projections.LastOrDefault(p =>
                p.Response.Subject == subject && p.Response.CeId == id && p.ResolvedAt is null);
            if (projection is not null)
            {
                projection.ResolvedAt = DateTimeOffset.UtcNow;
                projection.ResolvedBy = ResolvedBySystem;
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    /// <remarks>Returns null when no triggered incident matches the id.</remarks>
    public Task<IncidentResponse?> GetById(Guid id)
    {
        lock (_lock)
        {
            var projection = _projections.SingleOrDefault(p => p.Response.Id == id);
            return Task.FromResult(projection?.ToResponse());
        }
    }

    /// <inheritdoc/>
    /// <remarks>A null filter lists all recorded incidents. A null <c>Status</c> applies no status
    /// filtering; <c>Page</c> defaults to 1 and <c>PageSize</c> defaults to the result count.</remarks>
    public Task<IncidentListResponse> List(IncidentFilter? filter = null)
    {
        lock (_lock)
        {
            var query = _projections.AsEnumerable();

            if (!string.IsNullOrEmpty(filter?.Source))
            {
                query = query.Where(p => p.Response.Source == filter.Source);
            }

            if (!string.IsNullOrEmpty(filter?.Status))
            {
                query = query.Where(p =>
                    string.Equals(CurrentStatus(p), filter.Status, StringComparison.OrdinalIgnoreCase));
            }

            var items = query.ToList();
            var page = filter?.Page ?? 1;
            var pageSize = filter?.PageSize ?? items.Count;
            var paged = items.Skip((page - 1) * pageSize).Take(pageSize)
                .Select(p => p.ToResponse()).ToList();

            return Task.FromResult(new IncidentListResponse { Items = paged, Total = items.Count });
        }
    }

    /// <inheritdoc/>
    /// <remarks>Returns a <c>Triggered</c> event plus a <c>Resolved</c> event when the incident has
    /// been resolved; empty when no incident matches the id.</remarks>
    public Task<List<IncidentEventResponse>> GetEvents(Guid id)
    {
        lock (_lock)
        {
            var projection = _projections.SingleOrDefault(p => p.Response.Id == id);
            if (projection is null)
            {
                return Task.FromResult(new List<IncidentEventResponse>());
            }

            var events = new List<IncidentEventResponse>
            {
                new()
                {
                    Id = Guid.NewGuid(),
                    IncidentId = id,
                    EventType = EventTypes.Triggered,
                    Metadata = string.Empty,
                    OccurredAt = projection.Response.TriggeredAt,
                },
            };

            if (projection.ResolvedAt is not null)
            {
                events.Add(new IncidentEventResponse
                {
                    Id = Guid.NewGuid(),
                    IncidentId = id,
                    EventType = EventTypes.Resolved,
                    Metadata = string.Empty,
                    OccurredAt = projection.ResolvedAt.Value,
                });
            }

            return Task.FromResult(events);
        }
    }

    /// <inheritdoc/>
    /// <remarks>Marks the incident resolved by <c>manual</c>. Returns false when no incident matches
    /// the id.</remarks>
    public Task<bool> ResolveManual(Guid id)
    {
        lock (_lock)
        {
            var projection = _projections.SingleOrDefault(p => p.Response.Id == id);
            if (projection is null)
            {
                return Task.FromResult(false);
            }

            projection.ResolvedAt ??= DateTimeOffset.UtcNow;
            projection.ResolvedBy = "manual";
            return Task.FromResult(true);
        }
    }

    private static string CurrentStatus(Projection projection) =>
        projection.ResolvedAt is null ? StatusTriggered : StatusResolved;

    private sealed class Projection
    {
        public required IncidentResponse Response { get; init; }
        public DateTimeOffset? ResolvedAt { get; set; }
        public string? ResolvedBy { get; set; }

        public IncidentResponse ToResponse() => new()
        {
            Id = Response.Id,
            PreviousId = Response.PreviousId,
            Source = Response.Source,
            CeId = Response.CeId,
            Subject = Response.Subject,
            Description = Response.Description,
            Status = CurrentStatus(this),
            BatchId = Response.BatchId,
            Context = Response.Context,
            RetryCount = Response.RetryCount,
            TriggeredAt = Response.TriggeredAt,
            LastRetriedAt = Response.LastRetriedAt,
            ResolvedAt = ResolvedAt,
            ResolvedBy = ResolvedBy,
        };
    }
}
