using System.Globalization;
using System.Text;

namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// The key a payload is archived under, and the one place its format is defined.
/// </summary>
/// <remarks>
/// <para>
/// The format is <c>{component}/{yyyy}/{MM}/{dd}/{itemId}_{traceId}</c>. The date segments
/// partition the store for the lifecycle policy that expires copies. The item id is the message id
/// for a message and the file name for a swept file. The trace id names the trace the copy was
/// written in, so a copy found in the store leads back to its trace.
/// </para>
/// <para>
/// The key is stable across redeliveries of a message: the date comes from when the item was
/// received (or the event's time), not from the clock, and a redelivery continues the trace that
/// travels with the message. A redelivered message overwrites its own copy instead of adding one. A
/// file retried on the next sweep runs in a new trace, so each attempt keeps a copy of its own.
/// Without a trace, the trace id is left off.
/// </para>
/// <para>
/// Unlike a claim key, an archive key never travels as a pointer another component resolves, so it
/// is not validated on a way back in. The item id is made safe instead of refused: a file name with
/// a space must not stop its payload from being archived.
/// </para>
/// </remarks>
internal static class ArchiveKey
{
    /// <summary>The archive key for an item.</summary>
    /// <param name="componentName">The component archiving the payload.</param>
    /// <param name="date">The date the key is partitioned by.</param>
    /// <param name="itemId">The item's id: its message id, or its file name.</param>
    /// <param name="traceId">The id of the trace the copy is written in, if there is one.</param>
    /// <exception cref="InvalidOperationException">The component name cannot name a key
    /// segment.</exception>
    internal static string For(string componentName, DateTimeOffset date, string itemId, string? traceId)
    {
        RequireComponentCanName(componentName);
        ArgumentException.ThrowIfNullOrEmpty(itemId);

        var utc = date.UtcDateTime;
        var name = traceId is { Length: > 0 } ? $"{Safe(itemId)}_{Safe(traceId)}" : Safe(itemId);
        return string.Create(CultureInfo.InvariantCulture, $"{componentName}/{utc:yyyy}/{utc:MM}/{utc:dd}/{name}");
    }

    /// <summary>Refuses a component name the key format cannot hold. It is the one segment that is
    /// configuration rather than data, so it is refused, not rewritten: a rewritten name would file
    /// one component's copies under another's.</summary>
    /// <exception cref="InvalidOperationException">The component name cannot name a key
    /// segment.</exception>
    internal static void RequireComponentCanName(string? componentName)
    {
        if (!ClaimKey.CanName(componentName))
            throw new InvalidOperationException(
                $"The component name '{componentName}' cannot be archived under: it names the first segment " +
                "of every archive key, so it must be one or more letters, digits, '.', '-' or '_', and " +
                "neither '.' nor '..'.");
    }

    /// <summary><paramref name="value"/> with every character a segment cannot hold replaced by
    /// <c>_</c>, and <c>.</c> or <c>..</c> (which would traverse rather than name) as <c>_</c>.</summary>
    internal static string Safe(string value)
    {
        if (value is "." or "..")
            return "_";

        var safe = new StringBuilder(value.Length);
        foreach (var c in value)
            safe.Append(char.IsAsciiLetterOrDigit(c) || c is '.' or '-' or '_' ? c : '_');
        return safe.ToString();
    }
}
