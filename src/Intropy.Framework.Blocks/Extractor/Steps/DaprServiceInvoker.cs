using System.Net.Http.Headers;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Extractor.Steps;

internal static class CloudEventSerializer
{
    internal static readonly JsonEventFormatter Formatter = new();
}

/// <summary>
/// A step that sends CloudEvents to a Dapr service using service invocation.
/// This step sets the source and type on the CloudEvent from the configured values,
/// then serializes and invokes the target service's "ingest" endpoint with the CloudEvent as the request body.
/// </summary>
/// <typeparam name="TCtx">The type of the context.</typeparam>
/// <param name="daprClient">The Dapr client used to build the service invocation request.</param>
/// <param name="httpClient">The HTTP client used to send the request through the Dapr sidecar (typically created via <see cref="DaprClient.CreateInvokeHttpClient(string, string?, string?)"/>).</param>
/// <param name="appId">The Dapr app ID of the target service.</param>
/// <param name="source">The CloudEvent source URI identifying where the data came from.</param>
/// <param name="type">The CloudEvent type identifying the kind of event.</param>
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

        // Set source and type from configuration
        input.Source = source;
        input.Type = type;

        // Serialize CloudEvent to JSON
        var bytes = CloudEventSerializer.Formatter.EncodeStructuredModeMessage(input, out var contentType);

        // Build and send the service invocation request through the Dapr sidecar
        var request = daprClient.CreateInvokeMethodRequest(HttpMethod.Post, appId, MethodName);
        request.Content = new ByteArrayContent(bytes.ToArray());
        request.Content.Headers.ContentType = new MediaTypeHeaderValue(contentType.MediaType);

        await httpClient.SendAsync(request, ct);

        return (new TechnicalStepResult<CloudEvent>.Success(input), context);
    }
}
