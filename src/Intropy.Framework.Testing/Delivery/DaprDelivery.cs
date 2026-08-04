using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;

namespace Intropy.Framework.Testing.Delivery;

/// <summary>
/// Helpers for delivering <see cref="CloudEvent"/>s to loader subscription endpoints exactly as a
/// Dapr sidecar does, from a plain <see cref="HttpClient"/>.
/// </summary>
/// <remarks>
/// The Dapr HTTP delivery convention: the event is sent as a structured-mode CloudEvents envelope
/// with content type <c>application/cloudevents+json</c> (which ASP.NET's JSON binder rejects, so
/// subscription handlers parse the body manually; the payload sits in the envelope's
/// <c>data</c> member), and the endpoint answers with a <c>{"status": "..."}</c> ack body —
/// <c>SUCCESS</c> consumes the message, <c>RETRY</c> asks the sidecar for redelivery.
/// </remarks>
public static class DaprDelivery
{
    /// <summary>
    /// The content type of a structured-mode CloudEvents envelope.
    /// </summary>
    public const string CloudEventsContentType = "application/cloudevents+json";

    /// <summary>
    /// Encodes a <see cref="CloudEvent"/> as a structured-mode envelope JSON string — the same
    /// encoding the framework's Dapr topic publisher produces.
    /// </summary>
    /// <param name="cloudEvent">The event to encode.</param>
    /// <returns>The envelope JSON.</returns>
    public static string ToDeliveryEnvelope(CloudEvent cloudEvent)
    {
        ArgumentNullException.ThrowIfNull(cloudEvent);

        var formatter = new JsonEventFormatter();
        var bytes = formatter.EncodeStructuredModeMessage(cloudEvent, out _);
        return Encoding.UTF8.GetString(bytes.Span);
    }

    /// <summary>
    /// POSTs a <see cref="CloudEvent"/> to a subscription endpoint as a Dapr sidecar would and
    /// parses the <c>{"status": "..."}</c> ack body into a <see cref="DeliveryAck"/>.
    /// </summary>
    /// <param name="client">The HTTP client to deliver with (e.g. from
    /// <c>WebApplicationFactory.CreateClient()</c>).</param>
    /// <param name="route">The subscription endpoint route.</param>
    /// <param name="cloudEvent">The event to deliver.</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>The parsed ack. Unknown, missing, or malformed status values map to
    /// <see cref="DeliveryAck.Retry"/>, matching the sidecar's fail-safe redelivery.</returns>
    /// <exception cref="HttpRequestException">Thrown when the endpoint answers with a non-success
    /// status code — surfacing host errors in the test rather than masking them as retries.</exception>
    public static async Task<DeliveryAck> DeliverAsync(
        this HttpClient client, string route, CloudEvent cloudEvent, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(cloudEvent);

        using var content = new StringContent(
            ToDeliveryEnvelope(cloudEvent), Encoding.UTF8, CloudEventsContentType);

        using var response = await client.PostAsync(new Uri(route, UriKind.Relative), content, ct);
        response.EnsureSuccessStatusCode();

        var body = await response.Content.ReadAsStringAsync(ct);
        return ParseAck(body);
    }

    private static DeliveryAck ParseAck(string body)
    {
        string? status = null;

        if (!string.IsNullOrWhiteSpace(body))
        {
            try
            {
                using var document = JsonDocument.Parse(body);
                if (document.RootElement.ValueKind == JsonValueKind.Object &&
                    document.RootElement.TryGetProperty("status", out var statusElement) &&
                    statusElement.ValueKind == JsonValueKind.String)
                {
                    status = statusElement.GetString();
                }
            }
            catch (JsonException)
            {
                // Fall through: malformed ack bodies map to Retry, like the sidecar.
            }
        }

        return status?.ToUpperInvariant() switch
        {
            "SUCCESS" => DeliveryAck.Success,
            "DROP" => DeliveryAck.Drop,
            _ => DeliveryAck.Retry,
        };
    }
}
