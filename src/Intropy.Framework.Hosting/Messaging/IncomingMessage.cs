using System.Globalization;
using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf.WellKnownTypes;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// A message as the sidecar pushed it to the app callback: the CloudEvent's core attributes, its
/// data, and everything else (subject, time, trace context, propagated metadata) as extensions.
/// </summary>
/// <param name="MessageId">The CloudEvent id.</param>
/// <param name="PubSubName">The pub/sub component it was delivered from.</param>
/// <param name="TopicName">The topic it was delivered from.</param>
/// <param name="Source">The CloudEvent source.</param>
/// <param name="Type">The CloudEvent type.</param>
/// <param name="DataContentType">The CloudEvent data content type.</param>
/// <param name="Data">The CloudEvent data, as the sidecar encoded it.</param>
/// <param name="Extensions">The CloudEvent's other attributes.</param>
/// <param name="IsRetry">Whether the delivery is marked as a redelivery (a <c>retrycount</c>
/// extension). The sidecar's own retries carry no such mark.</param>
/// <param name="Path">The route the subscription's rules delivered it on, if any:
/// <see cref="UnhandledPath"/> when no rule selected it.</param>
internal sealed record IncomingMessage(
    string MessageId,
    string PubSubName,
    string TopicName,
    string? Source,
    string? Type,
    string? DataContentType,
    ReadOnlyMemory<byte> Data,
    IReadOnlyDictionary<string, Value> Extensions,
    bool IsRetry,
    string? Path = null)
{
    /// <summary>The default route of the component's rendered Dapr <c>Subscription</c>: the
    /// sidecar delivers here what none of its rules select — another type, or a handled type
    /// whose content filter did not match. Agrees with the topology's
    /// <c>SubscriptionRouting.UnhandledPath</c>.</summary>
    internal const string UnhandledPath = "/unhandled";

    private const string RetryCountKey = "retrycount";
    private static readonly IReadOnlyDictionary<string, Value> s_noExtensions = new Dictionary<string, Value>();

    /// <summary>Reads a delivery to the app callback.</summary>
    internal static IncomingMessage From(TopicEventRequest request)
    {
        IReadOnlyDictionary<string, Value> extensions = request.Extensions?.Fields ?? s_noExtensions;
        return new IncomingMessage(request.Id, request.PubsubName, request.Topic, request.Source, request.Type,
            request.DataContentType, request.Data.Memory, extensions, extensions.ContainsKey(RetryCountKey),
            string.IsNullOrEmpty(request.Path) ? null : request.Path);
    }

    /// <summary>Whether the subscription's rules selected none of its routes for this message.</summary>
    internal bool IsUnhandled => string.Equals(Path, UnhandledPath, StringComparison.Ordinal);

    /// <summary>The string value of extension <paramref name="key"/>, if it has one.</summary>
    internal string? GetExtension(string key) =>
        Extensions.TryGetValue(key, out var value) && value.KindCase == Value.KindOneofCase.StringValue
            ? value.StringValue
            : null;

    /// <summary>The message as a <see cref="CloudEvent"/>, with its data as text — what loader
    /// deserializers read.</summary>
    internal CloudEvent ToCloudEvent()
    {
        var cloudEvent = new CloudEvent
        {
            Id = MessageId,
            Type = Type,
            Data = DecodeData(Data)
        };

        if (!string.IsNullOrEmpty(DataContentType))
            cloudEvent.DataContentType = DataContentType;

        if (Uri.TryCreate(Source, UriKind.RelativeOrAbsolute, out var source))
            cloudEvent.Source = source;

        if (GetExtension("subject") is { Length: > 0 } subject)
            cloudEvent.Subject = subject;

        if (DateTimeOffset.TryParse(GetExtension("time"), CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind, out var time))
            cloudEvent.Time = time;

        return cloudEvent;
    }

    private static string DecodeData(ReadOnlyMemory<byte> data)
    {
        var text = Encoding.UTF8.GetString(data.Span);

        // The framework's blocks publish an object payload as a JSON object; a payload published
        // as a JSON string (pre-serialized text, or another publisher's) arrives quoted; unwrap it
        // to the payload itself.
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
