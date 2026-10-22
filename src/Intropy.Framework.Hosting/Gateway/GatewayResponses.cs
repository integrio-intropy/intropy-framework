using System.Globalization;
using Microsoft.AspNetCore.Http;

namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// The gateway's default answer to each <see cref="GatewayOutcome"/>. A component that answers
/// differently supplies its own mapping and may delegate the outcomes it keeps to
/// <see cref="Default"/>.
/// </summary>
/// <remarks>
/// <list type="table">
/// <item><term><see cref="GatewayOutcome.Published"/></term><description><c>202 Accepted</c>, the event id in the body.</description></item>
/// <item><term><see cref="GatewayOutcome.Duplicate"/></term><description><c>200 OK</c>.</description></item>
/// <item><term><see cref="GatewayOutcome.Rejected"/></term><description><c>422</c> problem details.</description></item>
/// <item><term><see cref="GatewayOutcome.TooLarge"/></term><description><c>413</c> problem details.</description></item>
/// <item><term><see cref="GatewayOutcome.Unavailable"/></term><description><c>503</c> problem details with <c>Retry-After</c>.</description></item>
/// </list>
/// Only <c>503</c> asks the caller to resend. The gateway sets <c>X-Request-Id</c> on every
/// response itself, whatever the mapping.
/// </remarks>
public static class GatewayResponses
{
    /// <summary>Maps <paramref name="outcome"/> to the gateway's default response.</summary>
    /// <param name="outcome">The request's outcome.</param>
    /// <returns>The response.</returns>
    public static IResult Default(GatewayOutcome outcome)
    {
        ArgumentNullException.ThrowIfNull(outcome);
        return outcome switch
        {
            GatewayOutcome.Published published => Results.Json(
                new PublishedBody(published.RequestId, published.EventId), statusCode: StatusCodes.Status202Accepted),
            GatewayOutcome.Duplicate duplicate => Results.Json(
                new DuplicateBody(duplicate.RequestId, Duplicate: true), statusCode: StatusCodes.Status200OK),
            GatewayOutcome.Rejected rejected => Problem(StatusCodes.Status422UnprocessableEntity,
                "The item was rejected", rejected.Description, rejected.RequestId),
            GatewayOutcome.TooLarge tooLarge => Problem(StatusCodes.Status413PayloadTooLarge,
                "The request body is too large", $"The gateway accepts at most {tooLarge.Limit} bytes.",
                tooLarge.RequestId),
            GatewayOutcome.Unavailable unavailable => new RetryAfterResult(
                Problem(StatusCodes.Status503ServiceUnavailable, "The gateway cannot take the item now",
                    unavailable.Description, unavailable.RequestId),
                unavailable.RetryAfter),
            _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome.GetType().Name,
                "Unknown gateway outcome."),
        };
    }

    private static IResult Problem(int status, string title, string? detail, string requestId) =>
        Results.Problem(detail: detail, statusCode: status, title: title,
            extensions: new Dictionary<string, object?> { ["requestId"] = requestId });

    /// <summary>The body of a <c>202</c>.</summary>
    /// <param name="RequestId">The request's identity.</param>
    /// <param name="EventId">The published CloudEvent's id.</param>
    public sealed record PublishedBody(string RequestId, string EventId);

    /// <summary>The body of a <c>200</c> for a duplicate.</summary>
    /// <param name="RequestId">The request's identity.</param>
    /// <param name="Duplicate">Always true.</param>
    public sealed record DuplicateBody(string RequestId, bool Duplicate);

    /// <summary>Sets <c>Retry-After</c>, in whole seconds, then writes the inner result.</summary>
    private sealed class RetryAfterResult(IResult inner, TimeSpan retryAfter) : IResult
    {
        public Task ExecuteAsync(HttpContext httpContext)
        {
            var seconds = Math.Max(1, (long)Math.Ceiling(retryAfter.TotalSeconds));
            httpContext.Response.Headers.RetryAfter = seconds.ToString(CultureInfo.InvariantCulture);
            return inner.ExecuteAsync(httpContext);
        }
    }
}
