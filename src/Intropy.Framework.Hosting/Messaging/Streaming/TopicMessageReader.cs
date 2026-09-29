using System.Globalization;
using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using Dapr.Messaging.PublishSubscribe;
using Google.Protobuf.WellKnownTypes;

namespace Intropy.Framework.Hosting.Messaging.Streaming;

/// <summary>
/// Reads a message delivered over a streaming subscription into the transport-independent
/// <see cref="IncomingMessage"/>. The stream carries the CloudEvent's core attributes as fields and
/// everything else (subject, time, trace context, the sidecar's <c>retrycount</c>) as extensions.
/// </summary>
internal static class TopicMessageReader
{
    private const string RetryCountKey = "retrycount";

    internal static IncomingMessage Read(TopicMessage message)
    {
        var cloudEvent = new CloudEvent
        {
            Id = message.Id,
            Type = message.Type,
            // Loader deserializers read the payload as text.
            Data = DecodeData(message.Data)
        };

        if (!string.IsNullOrEmpty(message.DataContentType))
            cloudEvent.DataContentType = message.DataContentType;

        if (Uri.TryCreate(message.Source, UriKind.RelativeOrAbsolute, out var source))
            cloudEvent.Source = source;

        if (GetString(message.Extensions, "subject") is { Length: > 0 } subject)
            cloudEvent.Subject = subject;

        if (DateTimeOffset.TryParse(GetString(message.Extensions, "time"), CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind, out var time))
            cloudEvent.Time = time;

        return new IncomingMessage(message.Id, cloudEvent, message.Extensions.ContainsKey(RetryCountKey));
    }

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

    private static string? GetString(IReadOnlyDictionary<string, Value> extensions, string key) =>
        extensions.TryGetValue(key, out var value) && value.KindCase == Value.KindOneofCase.StringValue
            ? value.StringValue
            : null;
}
