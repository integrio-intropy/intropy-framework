using System.Globalization;

namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// The key a claim-checked payload is filed under in the claim store, and the one place its format
/// is defined: the receive side mints a key, the send side validates a pointer against the same
/// shape before fetching it.
/// </summary>
/// <remarks>
/// <para>
/// The format is <c>{component}/{yyyy}/{MM}/{dd}/{messageId}</c>: unique per message, and named
/// after the message it travels on so a dead-lettered message's payload is findable. The date
/// segments partition the store for the lifecycle policy that expires payloads, which is the
/// platform's to own.
/// </para>
/// <para>
/// A key reaches the send side as a <c>dataref</c> on a message, which is why <see cref="IsValid"/>
/// exists. The file adapters join a key onto the store's base path without normalising it —
/// <see cref="Path.Combine(string, string)"/> drops the base entirely for an absolute path, and a
/// <c>..</c> segment climbs out of it whichever adapter joins it — so an unchecked pointer would let
/// anything able to publish to the internal topic name any file the binding can reach. Checking the
/// shape closes that: no absolute path, no traversal, and no separator a segment did not declare.
/// </para>
/// <para>
/// The component segment is checked for shape but not against the reading component's own name: one
/// component claiming a payload and another resolving it is a shape this format is meant to allow.
/// Within a store shared by several components that means one can read another's claim, which a
/// claim store per trust boundary is the deployment's answer to, not this check's.
/// </para>
/// </remarks>
internal static class ClaimKey
{
    /// <summary>The claim key for <paramref name="messageId"/>, written by
    /// <paramref name="componentName"/> at <paramref name="writtenAt"/>.</summary>
    /// <param name="componentName">The component filing the payload.</param>
    /// <param name="writtenAt">When the payload is written; the key is partitioned by its date.</param>
    /// <param name="messageId">The id of the message the pointer will travel on.</param>
    /// <exception cref="ArgumentException">A segment would not survive <see cref="IsValid"/>. The key
    /// would be written under a name its own reader rejects, so it fails here — before the payload is
    /// stored — rather than on the far side of the queue.</exception>
    internal static string For(string componentName, DateTimeOffset writtenAt, string messageId)
    {
        RequireSegment(componentName, nameof(componentName));
        RequireSegment(messageId, nameof(messageId));

        return string.Create(CultureInfo.InvariantCulture,
            $"{componentName}/{writtenAt:yyyy}/{writtenAt:MM}/{writtenAt:dd}/{messageId}");
    }

    /// <summary>Whether <paramref name="key"/> has the shape <see cref="For"/> mints.</summary>
    /// <param name="key">The pointer a message carries, or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the key is safe to hand to the store.</returns>
    internal static bool IsValid(string? key)
    {
        if (key is not { Length: > 0 })
            return false;

        var segments = key.Split('/');
        return segments.Length == 5
               && IsSegment(segments[0])
               && IsDigits(segments[1], 4)
               && IsDigits(segments[2], 2)
               && IsDigits(segments[3], 2)
               && IsSegment(segments[4]);
    }

    /// <summary>Whether <paramref name="segment"/> can name a part of a claim key, so a caller can
    /// refuse a value it did not choose before <see cref="For"/> throws over it.</summary>
    /// <param name="segment">The candidate segment — a component name, or the id of the message the
    /// pointer will travel on.</param>
    internal static bool CanName(string? segment) => segment is { Length: > 0 } s && IsSegment(s);

    /// <summary>One or more letters, digits, dots, dashes or underscores, and neither <c>.</c> nor
    /// <c>..</c>: the two that would traverse rather than name.</summary>
    private static bool IsSegment(string segment)
    {
        if (segment.Length == 0 || segment is "." or "..")
            return false;

        foreach (var c in segment)
            if (!char.IsAsciiLetterOrDigit(c) && c is not ('.' or '-' or '_'))
                return false;

        return true;
    }

    /// <summary>Exactly <paramref name="length"/> ASCII digits: a date segment.</summary>
    private static bool IsDigits(string segment, int length)
    {
        if (segment.Length != length)
            return false;

        foreach (var c in segment)
            if (!char.IsAsciiDigit(c))
                return false;

        return true;
    }

    private static void RequireSegment(string segment, string parameterName)
    {
        ArgumentException.ThrowIfNullOrEmpty(segment, parameterName);
        if (!IsSegment(segment))
            throw new ArgumentException(
                $"'{segment}' cannot name part of a claim key: a segment is one or more letters, digits, " +
                "'.', '-' or '_', and is neither '.' nor '..'.", parameterName);
    }
}
