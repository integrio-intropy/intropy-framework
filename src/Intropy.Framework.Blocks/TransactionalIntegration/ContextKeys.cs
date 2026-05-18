using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Blocks.TransactionalIntegration;

/// <summary>
/// Collection of constants that represent a key in the <see cref="Context"/> metadata dictionary
/// </summary>
public static class ContextKeys
{
    /// <summary>
    /// Key for locating the message id within the context metadata.
    /// </summary>
    public const string MessageId = "message_id";
}