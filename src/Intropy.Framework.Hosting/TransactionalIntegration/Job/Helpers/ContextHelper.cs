using System.Text.Json;
using Dapr.Messaging.PublishSubscribe;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Job.Helpers;

internal static class ContextHelper
{
    private const string MetadataKey = "metadata";
    private const string RetryCountKey = "retrycount";

    internal static Context Restore(TopicMessage message)
    {
        var metadata = GetMetadata(message.Extensions);
        metadata.TryAdd(ContextKeys.MessageId, message.Id);

        var isRetry = message.Extensions.ContainsKey(RetryCountKey);

        return new Context(metadata, isRetry);
    }

    private static Dictionary<string, string> GetMetadata(
        IReadOnlyDictionary<string, Value> extensions)
    {
        if (!extensions.TryGetValue(MetadataKey, out var value))
            return new Dictionary<string, string>();

        return JsonSerializer.Deserialize<Dictionary<string, string>>(value.StringValue)
               ?? new Dictionary<string, string>();
    }
}
