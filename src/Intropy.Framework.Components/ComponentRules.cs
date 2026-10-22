using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Components;

/// <summary>
/// How a business incident names the item it is about. Read from the context, because an incident
/// can be raised before the payload deserializes.
/// </summary>
/// <param name="Identity">The item's identity.</param>
/// <param name="Subject">The id a process owner recognizes.</param>
public sealed record IncidentRule(
    Func<Context, string> Identity,
    Func<Context, string> Subject)
{
    /// <summary>Names the incident after the HTTP request the item arrived in
    /// (<see cref="SourceContextKeys.RequestId"/>), which the gateway records on every context
    /// before any step runs — so even a body that cannot be deserialized is identifiable, and the
    /// caller holds the same id from the response's <c>X-Request-Id</c>.</summary>
    public static IncidentRule ByRequest { get; } = new(
        Identity: context => RequestId(context),
        Subject: context => $"request:{RequestId(context)}");

    /// <summary>Names the incident after the item's business identifier
    /// (<see cref="BusinessIncidentContextKeys.BusinessId"/>) once a step has set it, and after the
    /// request (<see cref="ByRequest"/>) before — so a body that cannot be deserialized is still
    /// identifiable, and an order that fails validation is named after the order.</summary>
    public static IncidentRule ByBusinessIdOrRequest { get; } = new(
        Identity: context => BusinessId(context) ?? RequestId(context),
        Subject: context => BusinessId(context) ?? $"request:{RequestId(context)}");

    private static string? BusinessId(Context context) =>
        context.Metadata.GetValueOrDefault(BusinessIncidentContextKeys.BusinessId) is { Length: > 0 } id ? id : null;

    private static string RequestId(Context context) =>
        context.Metadata.GetValueOrDefault(SourceContextKeys.RequestId, "unknown");
}
