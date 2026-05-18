using Dapr.Messaging.PublishSubscribe;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Blocks.TransactionalIntegration;
using Intropy.Framework.Hosting.TransactionalIntegration.Job.Helpers;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Job.Helpers;

public class ContextHelperTests
{
    [Fact]
    public void Restore_ShouldContext()
    {
        var extensions = new Dictionary<string, Value>
        {
            { "metadata", Value.ForString("{\"sourceItemId\":\"test.DAT\",\"file_name\":\"test.DAT\"}")}
        };
        var message = CreateTopicMessage("msg-id", extensions);

        var context = ContextHelper.Restore(message);

        Assert.Contains("sourceItemId", context.Metadata);
        Assert.Contains("file_name", context.Metadata);

        Assert.Equal("test.DAT",  context.Metadata["sourceItemId"]);
        Assert.Equal("test.DAT",  context.Metadata["file_name"]);
    }


    [Fact]
    public void Restore_ShouldExtractMessageId()
    {
        // Verifies that the message ID is correctly extracted and stored in context metadata
        var message = CreateTopicMessage("test-message-id");

        var context = ContextHelper.Restore(message);

        Assert.Equal("test-message-id", context.Metadata[ContextKeys.MessageId]);
    }

    [Fact]
    public void Restore_ShouldSetIsRetryTrue_WhenRetryCountExists()
    {
        // Verifies that retry messages are correctly identified from Dapr metadata
        var extensions = new Dictionary<string, Value>
        {
            { "retrycount", Value.ForString("1") }
        };
        var message = CreateTopicMessage("msg-id", extensions);

        var context = ContextHelper.Restore(message);

        Assert.True(context.IsRetry);
    }

    [Fact]
    public void Restore_ShouldSetIsRetryFalse_WhenRetryCountDoesNotExist()
    {
        // Verifies that first-time messages are correctly identified as non-retries
        var message = CreateTopicMessage("msg-id");

        var context = ContextHelper.Restore(message);

        Assert.False(context.IsRetry);
    }

    [Fact]
    public void Restore_ShouldCreateNewMetadataDictionary()
    {
        // Verifies that each context gets its own metadata dictionary to avoid sharing state
        var message1 = CreateTopicMessage("msg-1");
        var message2 = CreateTopicMessage("msg-2");

        var context1 = ContextHelper.Restore(message1);
        var context2 = ContextHelper.Restore(message2);

        Assert.NotSame(context1.Metadata, context2.Metadata);
        Assert.Equal("msg-1", context1.Metadata[ContextKeys.MessageId]);
        Assert.Equal("msg-2", context2.Metadata[ContextKeys.MessageId]);
    }



    private static TopicMessage CreateTopicMessage(string id, Dictionary<string, Value>? extensions = null)
    {
        return new TopicMessage(id, Source: "test-source", Type: "test-type", SpecVersion: "",
            DataContentType: "",
            Topic: "", PubSubName: "")
        {
            Data = "test-data"u8.ToArray(),
            Extensions = extensions ?? new Dictionary<string, Value>(),
        };
    }
}
