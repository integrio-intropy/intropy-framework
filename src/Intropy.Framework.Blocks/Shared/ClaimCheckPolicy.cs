using System.Diagnostics;
using CloudNative.CloudEvents;

namespace Intropy.Framework.Blocks.Shared;

/// <summary>When a payload is moved to the claim store instead of riding inside the envelope.</summary>
public enum ClaimCheckMode
{
    /// <summary>Inline below the threshold, claim-checked above it.</summary>
    Auto,

    /// <summary>Every payload goes through the claim store, whatever its size. For tests and for
    /// deployments that require payloads never to touch the broker.</summary>
    Always,

    /// <summary>
    /// No payload is claim-checked, whatever its size: every one rides inside its message.
    /// </summary>
    Never
}

/// <summary>
/// When to claim-check a payload, and the arithmetic that decides it.
/// </summary>
/// <remarks>
/// <para>
/// The threshold applies to the <em>encoded envelope</em>, not to the payload: what a broker rejects
/// is the message it is handed, attributes and encoding overhead included. The limit is the broker's
/// maximum message size — a deployment fact the framework cannot know — so it has no default and is
/// required in <see cref="ClaimCheckMode.Auto"/>.
/// </para>
/// <para>
/// <see cref="WouldExceed"/> measures without ever materialising the big encoding: it encodes the
/// envelope <em>without</em> its payload, which is cheap, and adds the payload's encoded length,
/// which is arithmetic. Encoding the real envelope to find out it is too large would base64 a large
/// file only to throw the result away.
/// </para>
/// <para>
/// A policy exists only where claim-checking happens, so <see cref="Mode"/> is never
/// <see cref="ClaimCheckMode.Never"/> — that mode means <em>no policy at all</em>, and the component's
/// options express it by producing none. The constructor is private so no caller can assemble a mode
/// and threshold that disagree with each other.
/// </para>
/// </remarks>
public sealed record ClaimCheckPolicy
{
    private ClaimCheckPolicy(ClaimCheckMode mode, int thresholdBytes)
    {
        Mode = mode;
        ThresholdBytes = thresholdBytes;
    }

    /// <summary>When to claim-check. Either <see cref="ClaimCheckMode.Auto"/> or
    /// <see cref="ClaimCheckMode.Always"/>; see the remarks for why never
    /// <see cref="ClaimCheckMode.Never"/>.</summary>
    public ClaimCheckMode Mode { get; }

    /// <summary>The largest envelope that may be published inline, in
    /// <see cref="ClaimCheckMode.Auto"/>. Ignored in <see cref="ClaimCheckMode.Always"/>.</summary>
    public int ThresholdBytes { get; }

    /// <summary>A policy that claim-checks every payload.</summary>
    public static ClaimCheckPolicy Always { get; } = new(ClaimCheckMode.Always, 0);

    /// <summary>A policy that claim-checks an envelope larger than <paramref name="thresholdBytes"/>.</summary>
    /// <param name="thresholdBytes">The largest envelope that may be published inline. Set it from the
    /// broker's maximum message size, with room to spare.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="thresholdBytes"/> is not positive.</exception>
    public static ClaimCheckPolicy Above(int thresholdBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(thresholdBytes);
        return new ClaimCheckPolicy(ClaimCheckMode.Auto, thresholdBytes);
    }

    /// <summary>Whether <paramref name="payloadLength"/> bytes of payload must go to the claim store
    /// rather than inside <paramref name="envelope"/>.</summary>
    /// <param name="envelope">The envelope as it would be published, with every attribute set but
    /// <see cref="CloudEvent.Data"/> left empty.</param>
    /// <param name="payloadLength">The payload's length in bytes, before encoding.</param>
    /// <param name="binary">Whether the payload is encoded as base64 (<c>data_base64</c>, the shape a
    /// <see cref="byte"/> array takes) rather than written as a JSON value.</param>
    /// <returns><see langword="true"/> when the payload must be claim-checked.</returns>
    public bool WouldExceed(CloudEvent envelope, int payloadLength, bool binary)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        return Mode switch
        {
            ClaimCheckMode.Always => true,
            ClaimCheckMode.Auto => EncodedSize(envelope, payloadLength, binary) > ThresholdBytes,
            _ => throw new UnreachableException($"No claim-check decision is defined for mode {Mode}")
        };
    }

    /// <summary>The size the published envelope would reach with a payload of
    /// <paramref name="payloadLength"/> bytes.</summary>
    /// <param name="envelope">The envelope with every attribute set and no data.</param>
    /// <param name="payloadLength">The payload's length in bytes, before encoding.</param>
    /// <param name="binary">Whether the payload is encoded as base64 rather than as a JSON value.</param>
    internal static long EncodedSize(CloudEvent envelope, int payloadLength, bool binary)
    {
        var overhead = CloudEventFormat.Formatter.EncodeStructuredModeMessage(envelope, out _).Length;
        return overhead + (binary ? Base64Length(payloadLength) : payloadLength);
    }

    /// <summary>The length base64 encoding gives <paramref name="byteCount"/> bytes, padding
    /// included.</summary>
    internal static long Base64Length(int byteCount) => 4L * ((byteCount + 2) / 3);
}
