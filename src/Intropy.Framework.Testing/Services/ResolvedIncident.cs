namespace Intropy.Framework.Testing.Services;

/// <summary>
/// A resolution recorded by <see cref="FakeBusinessIncidentServiceClient"/> when <c>Resolve</c> is
/// called.
/// </summary>
/// <param name="Source">The source URI of the resolved incident.</param>
/// <param name="Subject">The incident subject.</param>
/// <param name="Id">The incident identifier (the CloudEvent id).</param>
/// <param name="BatchId">The batch identifier, if any.</param>
public sealed record ResolvedIncident(
    Uri Source, string Subject, string Id, string? BatchId);
