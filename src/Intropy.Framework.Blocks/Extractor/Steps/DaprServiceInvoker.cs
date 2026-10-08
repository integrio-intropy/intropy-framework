using System.Net.Http.Headers;
using CloudNative.CloudEvents;
using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// A step that sends CloudEvents to a Dapr service using service invocation.
/// This step sets the source on the CloudEvent from the configured value, sets the type from
/// the configured value unless the serialize step already set one (a component-supplied type
/// extractor wins; the configured type is the fallback), then serializes and invokes the target
/// service's "ingest" endpoint with the CloudEvent as the request body.
/// </summary>
/// <remarks>
/// Only a success status code (2xx) counts as delivered. Any other status is a technical failure,
/// so the item is not completed and is retried; the idempotency check makes the retry safe.
/// </remarks>
/// <typeparam name="TCtx">The type of the context.</typeparam>
/// <param name="daprClient">The Dapr client used to build the service invocation request.</param>
/// <param name="httpClient">The HTTP client used to send the request through the Dapr sidecar (typically created via <see cref="DaprClient.CreateInvokeHttpClient(string, string?, string?)"/>).</param>
/// <param name="appId">The Dapr app ID of the target service.</param>
/// <param name="source">The CloudEvent source URI identifying where the data came from.</param>
/// <param name="type">The fallback CloudEvent type identifying the kind of event. Applies only when
/// the serialize step did not already set a type (e.g. via a component-supplied type extractor on
/// <see cref="CloudEventSerializeStep{TOutput,TCtx}"/>).</param>
public class DaprServiceInvoker<TCtx>(
    DaprClient daprClient,
    HttpClient httpClient,
    string appId,
    Uri source,
    string type) : SendStep<TCtx> where TCtx : Context
{
    private const string MethodName = "ingest";

    /// <inheritdoc/>
    public override async Task<(TechnicalStepResult<CloudEvent> Result, TCtx Context)> ExecuteAsync(CloudEvent input,
        TCtx context, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(input);

        // Validate required properties that should be set by the SerializeStep
        if (string.IsNullOrEmpty(input.Subject))
            throw new InvalidOperationException(
                "CloudEvent.Subject must be set by the SerializeStep. The subject identifies the entity this event is about.");

        if (input.Time is null)
            throw new InvalidOperationException(
                "CloudEvent.Time must be set by the SerializeStep. The time indicates when the event occurred in the source system.");

        // Set the source from configuration; the configured type is the fallback — a type set by
        // the serialize step (component-supplied type extractor) wins.
        input.Source = source;
        input.Type ??= type;

        // Serialize CloudEvent to JSON
        var bytes = CloudEventFormat.Formatter.EncodeStructuredModeMessage(input, out var contentType);

        // Build and send the service invocation request through the Dapr sidecar
        using var request = daprClient.CreateInvokeMethodRequest(HttpMethod.Post, appId, MethodName);
        request.Content = new ByteArrayContent(bytes.ToArray());
        request.Content.Headers.ContentType = new MediaTypeHeaderValue(contentType.MediaType);

        using var response = await httpClient.SendAsync(request, ct);

        // A rejected or failed invocation was not delivered: never report it as sent.
        if (!response.IsSuccessStatusCode)
        {
            var description = $"The service '{appId}' answered {(int)response.StatusCode} {response.ReasonPhrase}";
            return (new TechnicalStepResult<CloudEvent>.Failure(new TechnicalFailure(description,
                Exception: new HttpRequestException(description, null, response.StatusCode))), context);
        }

        return (new TechnicalStepResult<CloudEvent>.Success(input), context);
    }
}
