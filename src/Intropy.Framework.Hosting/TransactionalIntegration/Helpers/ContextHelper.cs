using System.Text.Json;
using Dapr.Messaging.PublishSubscribe;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration;

namespace Intropy.Framework.Hosting.TransactionalIntegration.Helpers;

internal static class ContextHelper
{
    private const string MetadataKey = "metadata";
    private const string RetryCountKey = "retrycount";

    /// <summary>Restores the context metadata propagated with <paramref name="message"/>, plus its
    /// message id, and whether it is a redelivery.</summary>
    internal static (Dictionary<string, string> Metadata, bool IsRetry) Restore(TopicMessage message)
    {
        var metadata = GetMetadata(message.Extensions);
        metadata.TryAdd(ContextKeys.MessageId, message.Id);

        return (metadata, message.Extensions.ContainsKey(RetryCountKey));
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
