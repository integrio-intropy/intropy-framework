using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.Test.Messaging;

/// <summary>
/// Reading one entry of a bulk delivery: the entry carries the event, the batch carries the pub/sub,
/// the topic and the route.
/// </summary>
public class IncomingMessageBulkEntryTests
{
    private static TopicEventBulkRequest Batch(string path = "/order.created") => new()
    {
        Id = "batch-1", PubsubName = "pubsub", Topic = "orders", Path = path, Type = "com.dapr.event.sent.bulk"
    };

    private static TopicEventBulkRequestEntry CloudEventEntry(IDictionary<string, string>? extensions = null)
    {
        var cloudEvent = new TopicEventCERequest
        {
            Id = "msg-1", Source = "urn:test", Type = "order.created", SpecVersion = "1.0",
            DataContentType = "application/json", Data = ByteString.CopyFromUtf8("{\"orderId\":\"ORD-1\"}"),
            Extensions = new Struct()
        };
        foreach (var (key, value) in extensions ?? new Dictionary<string, string>())
            cloudEvent.Extensions.Fields[key] = Value.ForString(value);
        return new TopicEventBulkRequestEntry
        {
            EntryId = "entry-1", CloudEvent = cloudEvent, ContentType = "application/cloudevents+json"
        };
    }

    [Fact]
    public void FromBulkEntry_ReadsTheEventFromTheEntry_AndTheChannelFromTheBatch()
    {
        var message = IncomingMessage.FromBulkEntry(CloudEventEntry(new Dictionary<string, string>
        {
            ["subject"] = "ORD-1"
        }), Batch());

        Assert.NotNull(message);
        Assert.Equal("msg-1", message.MessageId);
        Assert.Equal("order.created", message.Type);
        Assert.Equal("urn:test", message.Source);
        Assert.Equal("application/json", message.DataContentType);
        Assert.Equal("{\"orderId\":\"ORD-1\"}", System.Text.Encoding.UTF8.GetString(message.Data.Span));
        Assert.Equal("ORD-1", message.Subject);
        Assert.Equal("pubsub", message.PubSubName);
        Assert.Equal("orders", message.TopicName);
        Assert.Equal("/order.created", message.Path);
        Assert.False(message.IsRetry);
    }

    [Fact]
    public void FromBulkEntry_MarksARedelivery()
    {
        var message = IncomingMessage.FromBulkEntry(CloudEventEntry(new Dictionary<string, string>
        {
            ["retrycount"] = "1"
        }), Batch());

        Assert.True(message!.IsRetry);
    }

    [Fact]
    public void FromBulkEntry_ReadsTheDefaultRouteAsUnhandled()
    {
        var message = IncomingMessage.FromBulkEntry(CloudEventEntry(), Batch(IncomingMessage.UnhandledPath));

        Assert.True(message!.IsUnhandled);
    }

    [Fact]
    public void FromBulkEntry_ReturnsNull_ForAnEntryDeliveredAsRawBytes()
    {
        // A raw-payload subscription delivers bytes, not a CloudEvent: there is no type to route on.
        var entry = new TopicEventBulkRequestEntry
        {
            EntryId = "entry-1", Bytes = ByteString.CopyFromUtf8("{}"), ContentType = "application/json"
        };

        Assert.Null(IncomingMessage.FromBulkEntry(entry, Batch()));
    }
}
