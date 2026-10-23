using System.Net;
using System.Net.Sockets;
using Dapr;
using Grpc.Core;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Hosting.Jobs;
using TechnicalFailure = Intropy.Framework.Core.Pipeline.Abstractions.Failures.TechnicalFailure;

namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// Tells a technical failure a resend may cure from one it cannot. The caller is the gateway's
/// only retry mechanism, so a <c>503</c> for a failure that is certain to recur — a step that
/// throws on every item, an output event without a subject — makes the caller resend forever.
/// </summary>
/// <remarks>
/// Transient is an allowlist, judged on the failure's exception and its inner exceptions: an
/// unreachable or failing sidecar, pub/sub or platform service (gRPC <c>Unavailable</c>,
/// <c>DeadlineExceeded</c>, <c>ResourceExhausted</c>, <c>Aborted</c>, <c>Internal</c>,
/// <c>Unknown</c>; an HTTP failure without a status or with a <c>5xx</c>, <c>408</c> or
/// <c>429</c>; a timeout, socket or IO error; a Dapr or platform service client failure). Anything
/// else — a failure without an exception, or a fault in the component's own code — is permanent.
/// </remarks>
internal static class GatewayFailures
{
    public static bool IsTransient(TechnicalFailure failure)
    {
        var sawDapr = false;
        for (var exception = failure.Exception; exception is not null; exception = Inner(exception))
        {
            switch (exception)
            {
                case RpcException rpc:
                    return rpc.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded
                        or StatusCode.ResourceExhausted or StatusCode.Aborted or StatusCode.Internal
                        or StatusCode.Unknown;
                case HttpRequestException http:
                    return http.StatusCode is null or HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests
                        || (int)http.StatusCode >= 500;
                case TimeoutException or OperationCanceledException or IOException or SocketException
                    or InfrastructureUnavailableException:
                    return true;
                case IdempotencyServiceException or BusinessIncidentServiceException:
                    // The platform services are the system's own infrastructure: a failure reaching or
                    // answering them is an outage, not a property of the item.
                    return true;
                case DaprException:
                    // Its inner exception, when there is one, says more; a bare one is the sidecar's.
                    sawDapr = true;
                    break;
            }
        }

        return sawDapr;
    }

    private static Exception? Inner(Exception exception) =>
        exception is AggregateException { InnerExceptions.Count: 1 } aggregate
            ? aggregate.InnerExceptions[0]
            : exception.InnerException;
}
