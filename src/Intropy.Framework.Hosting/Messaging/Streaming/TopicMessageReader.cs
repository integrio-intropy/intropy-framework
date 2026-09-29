using System.Globalization;
using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using Dapr.Messaging.PublishSubscribe;
using Google.Protobuf.WellKnownTypes;

namespace Intropy.Framework.Hosting.Messaging.Streaming;

/// <summary>
/// Reads a message delivered over a streaming subscription (or the gRPC app callback) into the transport-independent
/// <see cref="IncomingMessage"/>. The stream carries the CloudEvent's core attributes as fields and
/// everything else (subject, time, trace context, the sidecar's <c>retrycount</c>) as extensions.
/// </summary>
internal static class TopicMessageReader
{
    private const string RetryCountKey = "retrycount";

    internal static IncomingMessage Read(TopicMessage message) =>
        Read(message.Id, message.Source, message.Type, message.DataContentType, message.Data, message.Extensions,
            message.Extensions.ContainsKey(RetryCountKey));

    /// <summary>Reads a CloudEvent delivered as its attributes, data and extensions — the shape both
    /// the streaming subscription and the gRPC app callback use.</summary>
    internal static IncomingMessage Read(string id, string? source, string? type, string? dataContentType,
        ReadOnlyMemory<byte> data, IReadOnlyDictionary<string, Value> extensions, bool isRetry)
    {
        var cloudEvent = new CloudEvent
        {
            Id = id,
            Type = type,
            // Loader deserializers read the payload as text.
            Data = DecodeData(data)
        };

        if (!string.IsNullOrEmpty(dataContentType))
            cloudEvent.DataContentType = dataContentType;

        if (Uri.TryCreate(source, UriKind.RelativeOrAbsolute, out var parsedSource))
            cloudEvent.Source = parsedSource;

        if (GetString(extensions, "subject") is { Length: > 0 } subject)
            cloudEvent.Subject = subject;

        if (DateTimeOffset.TryParse(GetString(extensions, "time"), CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind, out var time))
            cloudEvent.Time = time;

        return new IncomingMessage(id, cloudEvent, isRetry);
    }

    /// <summary>The string value of extension <paramref name="key"/>, if it has one.</summary>
    internal static string? GetString(IReadOnlyDictionary<string, Value> extensions, string key) =>
        extensions.TryGetValue(key, out var value) && value.KindCase == Value.KindOneofCase.StringValue
            ? value.StringValue
            : null;

    private static string DecodeData(ReadOnlyMemory<byte> data)
    {
        var text = Encoding.UTF8.GetString(data.Span);

        // A payload published as a JSON string (as the framework's CloudEvent serializer does)
        // arrives quoted; unwrap it to the payload itself.
        if (text.StartsWith('"'))
        {
            try
            {
                return JsonSerializer.Deserialize<string>(text) ?? text;
            }
            catch (JsonException)
            {
                // Not a JSON string after all: hand the text over as it is.
            }
        }

        return text;
    }

}
