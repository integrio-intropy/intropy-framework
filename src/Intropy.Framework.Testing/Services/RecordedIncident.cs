using Intropy.Contracts.BusinessIncidentService;

namespace Intropy.Framework.Testing.Services;

/// <summary>
/// A business incident recorded by <see cref="FakeBusinessIncidentServiceClient"/> when
/// <c>Trigger</c> is called.
/// </summary>
/// <param name="Source">The source URI of the incident.</param>
/// <param name="Subject">The incident subject.</param>
/// <param name="Id">The incident identifier (the CloudEvent id).</param>
/// <param name="Data">The incident payload.</param>
/// <param name="BatchId">The batch identifier, if any.</param>
public sealed record RecordedIncident(
    Uri Source, string Subject, string Id, BusinessIncidentData Data, string? BatchId);
