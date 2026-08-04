using System.Net;
using System.Text;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using Intropy.Framework.Testing.Delivery;

namespace Intropy.Framework.Testing.Test.Delivery;

public class DaprDeliveryTests
{
    private static CloudEvent NewEvent() => new()
    {
        Id = "ce-1",
        Source = new Uri("urn:test-component"),
        Type = "com.example.order",
        Subject = "order-42",
        Time = new DateTimeOffset(2026, 1, 15, 10, 30, 0, TimeSpan.Zero),
        DataContentType = "application/json",
        Data = new { OrderId = 42 },
    };

    [Fact]
    public void ToDeliveryEnvelope_RoundTripsThroughJsonEventFormatter()
    {
        var cloudEvent = NewEvent();

        var envelope = DaprDelivery.ToDeliveryEnvelope(cloudEvent);
        var decoded = new JsonEventFormatter().DecodeStructuredModeMessage(
            Encoding.UTF8.GetBytes(envelope), contentType: null, extensionAttributes: null);

        Assert.Equal(cloudEvent.Id, decoded.Id);
        Assert.Equal(cloudEvent.Subject, decoded.Subject);
        Assert.Equal(cloudEvent.Time, decoded.Time);
        Assert.NotNull(decoded.Data);
    }

    [Theory]
    [InlineData("{\"status\":\"SUCCESS\"}", DeliveryAck.Success)]
    [InlineData("{\"status\":\"RETRY\"}", DeliveryAck.Retry)]
    [InlineData("{\"status\":\"DROP\"}", DeliveryAck.Drop)]
    [InlineData("{\"status\":\"success\"}", DeliveryAck.Success)]
    [InlineData("{\"status\":\"WHATEVER\"}", DeliveryAck.Retry)]
    [InlineData("{\"status\":42}", DeliveryAck.Retry)]
    [InlineData("{\"other\":\"field\"}", DeliveryAck.Retry)]
    [InlineData("", DeliveryAck.Retry)]
    [InlineData("not json at all", DeliveryAck.Retry)]
    public async Task DeliverAsync_ParsesAckBody_WithFailSafeRetry(string ackBody, DeliveryAck expected)
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(ackBody, Encoding.UTF8, "application/json"),
        });
        using var client = new HttpClient(handler) { BaseAddress = new Uri("http://localhost") };

        var ack = await client.DeliverAsync("/events/orders", NewEvent());

        Assert.Equal(expected, ack);
    }

    [Fact]
    public async Task DeliverAsync_PostsStructuredEnvelopeWithCloudEventsContentType()
    {
        HttpRequestMessage? captured = null;
        string? body = null;
        var handler = new StubHandler(request =>
        {
            captured = request;
            body = request.Content!.ReadAsStringAsync().GetAwaiter().GetResult();
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{\"status\":\"SUCCESS\"}"),
            };
        });
        using var client = new HttpClient(handler) { BaseAddress = new Uri("http://localhost") };

        await client.DeliverAsync("/events/orders", NewEvent());

        Assert.NotNull(captured);
        Assert.Equal("application/cloudevents+json", captured.Content!.Headers.ContentType!.MediaType);
        Assert.Equal("/events/orders", captured.RequestUri!.AbsolutePath);
        var decoded = new JsonEventFormatter().DecodeStructuredModeMessage(
            Encoding.UTF8.GetBytes(body!), contentType: null, extensionAttributes: null);
        Assert.Equal("ce-1", decoded.Id);
    }

    [Fact]
    public async Task DeliverAsync_NonSuccessStatusCode_ThrowsHttpRequestException()
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError));
        using var client = new HttpClient(handler) { BaseAddress = new Uri("http://localhost") };

        await Assert.ThrowsAsync<HttpRequestException>(
            () => client.DeliverAsync("/events/orders", NewEvent()));
    }

    private sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> respond)
        : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(respond(request));
    }
}
