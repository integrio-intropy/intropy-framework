namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// The dictionary key on the pipeline context that a step sets to name its item for business
/// incidents.
/// </summary>
/// <remarks>
/// A routed incident ends the pipeline as a success — the successful result's
/// <see cref="Intropy.Framework.Core.Pipeline.Abstractions.Results.StepResult{T}.RoutedIncident"/> subtype carries the
/// routed incident — so a host that only completes or deletes its item treats it like the handled
/// item it is, and a host that answers a caller matches the subtype to tell it apart from a publish.
/// </remarks>
public static class BusinessIncidentContextKeys
{
    /// <summary>
    /// The item's business identifier — an order number, a customer id — set by the step that first
    /// knows it (typically the deserializer), so an incident raised from then on is named after it.
    /// Absent until a step sets it.
    /// </summary>
    public const string BusinessId = "business_id";
}
