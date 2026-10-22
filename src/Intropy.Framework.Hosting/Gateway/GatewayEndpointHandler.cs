using System.Diagnostics;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Extractor;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// One endpoint of a gateway: handles a request to its path by reading the body, running it through
/// the endpoint's pipeline in the request's scope, and answering with the outcome. It is the
/// gateway's counterpart of a file sweep's per-file handling — same pipeline, same context keys —
/// except that the caller, not a next run, is the retry mechanism, so every result becomes an answer.
/// </summary>
/// <remarks>
/// <para>
/// Context: <see cref="SourceContextKeys.SourcePort"/>, <see cref="SourceContextKeys.ReceivedAt"/>
/// and <see cref="SourceContextKeys.RequestId"/> are set before any step runs, so incident routing
/// can name a body that does not deserialize.
/// </para>
/// <para>
/// Tracing: the request is a server span on the framework's source, continuing the ASP.NET Core
/// request span when one is recorded and the caller's <c>traceparent</c> otherwise. The pipeline
/// runs with <c>detachTrace: false</c>, so the published event carries the caller's trace onward.
/// </para>
/// </remarks>
internal sealed class GatewayEndpointHandler
{
    /// <summary>The header a caller may name its request with; echoed on every response.</summary>
    internal const string RequestIdHeader = "X-Request-Id";

    private const int MaxRequestIdLength = 128;

    private readonly ExtractorRoute<Context> _route;
    private readonly Func<GatewayOutcome, IResult> _respond;
    private readonly GatewayOptions _options;
    private readonly GatewayReadiness _readiness;
    private readonly TimeProvider _time;
    private readonly ILogger<GatewayEndpointHandler> _logger;

    public GatewayEndpointHandler(ExtractorRoute<Context> route, string port, string path,
        Func<GatewayOutcome, IResult> respond, GatewayOptions options, GatewayReadiness readiness,
        FrameworkOptions frameworkOptions, TimeProvider time, ILogger<GatewayEndpointHandler> logger)
    {
        _route = route;
        Port = port;
        Path = path;
        _respond = respond;
        _options = options;
        _readiness = readiness;
        ComponentName = frameworkOptions.ComponentName;
        _time = time;
        _logger = logger;
    }

    public string ComponentName { get; }

    public string Port { get; }

    public string Path { get; }

    /// <summary>Composes the pipeline once in <paramref name="scope"/>; throws what composition
    /// would throw on the first request.</summary>
    public void Validate(IServiceProvider scope) => _ = _route.Resolve(scope, ComponentName, Port);

    /// <summary>Handles the request and writes the response.</summary>
    public async Task HandleAsync(HttpContext http)
    {
        var start = Stopwatch.GetTimestamp();
        var requestId = RequestId(http.Request);
        http.Response.Headers[RequestIdHeader] = requestId;

        using var activity = StartActivity(http.Request, requestId);
        var outcome = await DecideAsync(http, requestId);

        activity?.SetTag("intropy.gateway.outcome", OutcomeName(outcome));
        if (outcome is GatewayOutcome.Unavailable unavailable)
            activity?.SetStatus(ActivityStatusCode.Error, unavailable.Description);
        else if (outcome is GatewayOutcome.Failed failed)
            activity?.SetStatus(ActivityStatusCode.Error, failed.Description);
        HostingMetrics.RecordGatewayRequest(ComponentName, Port, OutcomeName(outcome),
            Stopwatch.GetElapsedTime(start));

        await _respond(outcome).ExecuteAsync(http);
    }

    private async Task<GatewayOutcome> DecideAsync(HttpContext http, string requestId)
    {
        if (!_readiness.IsReady)
            return new GatewayOutcome.Unavailable(requestId, "The gateway is not ready: its sidecar is not available yet.",
                _options.RetryAfter);

        var body = await ReadBodyAsync(http.Request, http.RequestAborted);
        if (body is null)
        {
            _logger.LogWarning("Rejected request {RequestId}: the body exceeds {Limit} bytes", requestId,
                _options.MaxRequestBodyBytes);
            return new GatewayOutcome.TooLarge(requestId, _options.MaxRequestBodyBytes);
        }

        var context = ContextCreation.Create<Context>(static (metadata, isRetry) => new Context(metadata, isRetry),
            new Dictionary<string, string>
            {
                [SourceContextKeys.SourcePort] = Port,
                [SourceContextKeys.ReceivedAt] = _time.GetUtcNow().ToString("O"),
                [SourceContextKeys.RequestId] = requestId,
            },
            isRetry: false, ComponentName, requestId);

        StepResult<CloudEvent> result;
        try
        {
            var run = _route.Resolve(http.RequestServices, ComponentName, Port);
            result = await run(body.Value, context, http.RequestAborted);
        }
        catch (OperationCanceledException) when (http.RequestAborted.IsCancellationRequested)
        {
            return new GatewayOutcome.Unavailable(requestId, "The caller aborted the request.", _options.RetryAfter);
        }

        return Outcome(result, requestId);
    }

    private GatewayOutcome Outcome(StepResult<CloudEvent> result, string requestId)
    {
        switch (result)
        {
            // A routed incident also ends as a success; the result's subtype tells them apart.
            case StepResult<CloudEvent>.RoutedIncident routed:
                _logger.LogInformation("Rejected request {RequestId}: {Description}", requestId, routed.Description);
                return new GatewayOutcome.Rejected(requestId, routed.Description);
            case StepResult<CloudEvent>.Success success:
                _logger.LogInformation("Published request {RequestId} as event {EventId}", requestId, success.Value.Id);
                return new GatewayOutcome.Published(requestId, success.Value.Id ?? "");
            case StepResult<CloudEvent>.Cancelled:
                _logger.LogInformation("Request {RequestId} is a duplicate (idempotency check)", requestId);
                return new GatewayOutcome.Duplicate(requestId);
            case StepResult<CloudEvent>.BusinessFailure failure:
                return new GatewayOutcome.Rejected(requestId, failure.Value.Description);
            case StepResult<CloudEvent>.TechnicalFailure failure when GatewayFailures.IsTransient(failure.Value):
                _logger.LogWarning(failure.Value.Exception, "Request {RequestId} failed: {Description}", requestId,
                    failure.Value.Description);
                return new GatewayOutcome.Unavailable(requestId, failure.Value.Description, _options.RetryAfter);
            case StepResult<CloudEvent>.TechnicalFailure failure:
                // Resending would fail the same way: answer 500, so the caller does not loop.
                _logger.LogError(failure.Value.Exception, "Request {RequestId} failed permanently: {Description}",
                    requestId, failure.Value.Description);
                return new GatewayOutcome.Failed(requestId, failure.Value.Description);
            default:
                _logger.LogWarning("Request {RequestId} was interrupted ({ResultType})", requestId,
                    result.GetType().Name);
                return new GatewayOutcome.Unavailable(requestId, "Processing was interrupted.", _options.RetryAfter);
        }
    }

    /// <summary>Reads the body's bytes, undecoded: the route's deserializer owns the encoding. Null
    /// when it exceeds <see cref="GatewayOptions.MaxRequestBodyBytes"/>.</summary>
    private async Task<ReadOnlyMemory<byte>?> ReadBodyAsync(HttpRequest request, CancellationToken ct)
    {
        var limit = _options.MaxRequestBodyBytes;
        if (request.ContentLength > limit)
            return null;

        using var buffer = new MemoryStream();
        var chunk = new byte[16 * 1024];
        int read;
        while ((read = await request.Body.ReadAsync(chunk, ct)) > 0)
        {
            if (buffer.Length + read > limit)
                return null;
            await buffer.WriteAsync(chunk.AsMemory(0, read), ct);
        }

        return buffer.GetBuffer().AsMemory(0, (int)buffer.Length);
    }

    private static string RequestId(HttpRequest request)
    {
        var header = request.Headers[RequestIdHeader].ToString().Trim();
        return header.Length is > 0 and <= MaxRequestIdLength ? header : Guid.NewGuid().ToString("N");
    }

    private Activity? StartActivity(HttpRequest request, string requestId)
    {
        // Continue the ASP.NET Core request span when one is recorded; otherwise the caller's
        // traceparent, so the trace survives a strict telemetry mode that drops ASP.NET Core spans.
        var parent = default(ActivityContext);
        if (Activity.Current is null)
            ActivityContext.TryParse(request.Headers.TraceParent, request.Headers.TraceState, out parent);

        var activity = ActivitySourceProvider.ActivitySource.StartActivity($"{ComponentName} receive",
            ActivityKind.Server, parent);
        activity?.SetTag("intropy.component.name", ComponentName);
        activity?.SetTag("intropy.source.port", Port);
        activity?.SetTag("intropy.gateway.request_id", requestId);
        return activity;
    }

    private static string OutcomeName(GatewayOutcome outcome) => outcome switch
    {
        GatewayOutcome.Published => "published",
        GatewayOutcome.Duplicate => "duplicate",
        GatewayOutcome.Rejected => "rejected",
        GatewayOutcome.TooLarge => "too_large",
        GatewayOutcome.Unavailable => "unavailable",
        GatewayOutcome.Failed => "failed",
        _ => "unknown",
    };
}
