using System.Net;
using System.Net.Sockets;
using Dapr;
using Grpc.Core;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Hosting.Gateway;

namespace Intropy.Framework.Hosting.Test.Gateway;

/// <summary>
/// The gateway answers a transient technical failure with 503 and every other one with 500, so a
/// caller never resends an item that fails the same way each time.
/// </summary>
public class GatewayFailuresTests
{
    public static TheoryData<Exception> TransientExceptions => new()
    {
        new HttpRequestException("connection refused"),
        new HttpRequestException("bad gateway", null, HttpStatusCode.BadGateway),
        new HttpRequestException("throttled", null, HttpStatusCode.TooManyRequests),
        new TimeoutException(),
        new TaskCanceledException("HttpClient timeout"),
        new IOException("reset"),
        new SocketException(),
        new IdempotencyServiceException("unavailable", new HttpRequestException()),
        new BusinessIncidentServiceException("unavailable", new InvalidOperationException()),
        new DaprException("Publish operation failed", new RpcException(new Status(StatusCode.Unavailable, "down"))),
        new DaprException("Publish operation failed", new RpcException(new Status(StatusCode.Internal, "broker"))),
        new DaprException("sidecar failed"),
        // The idempotency recorder wraps the client's failure; the inner exception still decides.
        new InvalidOperationException("Failed to record", new IdempotencyServiceException("unavailable")),
    };

    public static TheoryData<Exception> PermanentExceptions => new()
    {
        new InvalidOperationException("CloudEvent.Subject must be set by the SerializeStep."),
        new NotImplementedException(),
        new NullReferenceException(),
        new ArgumentException("bad"),
        new HttpRequestException("bad request", null, HttpStatusCode.BadRequest),
        new DaprException("Publish operation failed",
            new RpcException(new Status(StatusCode.InvalidArgument, "pubsub not found"))),
    };

    [Theory]
    [MemberData(nameof(TransientExceptions))]
    public void IsTransient_WithAnInfrastructureFailure_IsTrue(Exception exception)
    {
        var failure = new TechnicalFailure("failed", Exception: exception);

        Assert.True(GatewayFailures.IsTransient(failure));
    }

    [Theory]
    [MemberData(nameof(PermanentExceptions))]
    public void IsTransient_WithAFaultInTheComponent_IsFalse(Exception exception)
    {
        var failure = new TechnicalFailure("failed", Exception: exception);

        Assert.False(GatewayFailures.IsTransient(failure));
    }

    [Fact]
    public void IsTransient_WithoutAnException_IsFalse()
    {
        var failure = new TechnicalFailure("CloudEvent.Subject must be set by the SerializeStep.");

        Assert.False(GatewayFailures.IsTransient(failure));
    }
}
