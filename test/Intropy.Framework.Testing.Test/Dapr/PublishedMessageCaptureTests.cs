using CloudNative.CloudEvents;
using Intropy.Framework.Testing.Dapr;

namespace Intropy.Framework.Testing.Test.Dapr;

public class PublishedMessageCaptureTests
{
    [Fact]
    public void Capture_RecordsMessagesInOrder_WithDecodedEnvelope()
    {
        var capture = new PublishedMessageCapture();
        var cloudEvent = new CloudEvent
        {
            Id = "ce-1",
            Source = new Uri("urn:test"),
            Type = "com.example.order",
            Subject = "order-42",
            Time = DateTimeOffset.UtcNow,
        };
        var envelope = System.Text.Encoding.UTF8.GetBytes(
            Testing.Delivery.DaprDelivery.ToDeliveryEnvelope(cloudEvent));

        capture.Capture("pubsub", "orders", envelope, "application/cloudevents+json");
        capture.Capture("pubsub", "orders-deadletter", ReadOnlyMemory<byte>.Empty, null);

        Assert.Equal(2, capture.Count);

        var first = capture.Messages[0];
        Assert.Equal("pubsub", first.PubSubName);
        Assert.Equal("orders", first.TopicName);
        Assert.Equal("application/cloudevents+json", first.ContentType);
        Assert.Equal("order-42", first.DecodeCloudEvent().Subject);
        Assert.Equal("ce-1", first.DecodeCloudEvent().Id);

        var second = capture.Messages[1];
        Assert.Equal("orders-deadletter", second.TopicName);
        Assert.Null(second.ContentType);
    }

    [Fact]
    public void GetDataAsString_DecodesUtf8()
    {
        var capture = new PublishedMessageCapture();
        capture.Capture("pubsub", "topic", System.Text.Encoding.UTF8.GetBytes("payload"), "text/plain");

        Assert.Equal("payload", capture.Messages.Single().GetDataAsString());
    }

    [Fact]
    public void Messages_ReturnsSnapshot_UnaffectedByLaterCaptures()
    {
        var capture = new PublishedMessageCapture();
        capture.Capture("pubsub", "topic", ReadOnlyMemory<byte>.Empty);

        var snapshot = capture.Messages;
        capture.Capture("pubsub", "topic", ReadOnlyMemory<byte>.Empty);

        Assert.Single(snapshot);
        Assert.Equal(2, capture.Count);
    }
}
