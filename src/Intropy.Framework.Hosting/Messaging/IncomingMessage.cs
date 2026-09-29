using CloudNative.CloudEvents;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>A consumed message, independent of the transport that delivered it.</summary>
/// <param name="MessageId">The transport's id for the message.</param>
/// <param name="CloudEvent">The CloudEvent the routes consume.</param>
/// <param name="IsRetry">Whether the transport marked it as a redelivery.</param>
internal sealed record IncomingMessage(string MessageId, CloudEvent CloudEvent, bool IsRetry);
