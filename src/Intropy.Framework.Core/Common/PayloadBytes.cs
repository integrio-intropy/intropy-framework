using System.Runtime.InteropServices;

namespace Intropy.Framework.Core.Common;

/// <summary>
/// Turns a payload handed over as <see cref="ReadOnlyMemory{T}"/> into a <see cref="byte"/> array
/// without copying it when it already is one.
/// </summary>
/// <remarks>
/// <para>
/// A payload that came through Dapr — a binding read, a pub/sub message — is a protobuf
/// <c>ByteString</c>, whose memory is backed by an array of exactly the payload's length.
/// <see cref="ReadOnlyMemory{T}.ToArray"/> copies it regardless, which for a 10 MB file is another
/// 10 MB on the large object heap. <see cref="AsArray"/> hands back that backing array instead, and
/// copies only when the memory is a slice of something larger or not backed by an array at all.
/// </para>
/// <para>
/// The array may be the one the memory wraps, so treat it as read-only: writing to it changes the
/// payload for everything else holding the same memory.
/// </para>
/// </remarks>
/// <example>
/// In a deserialize step, instead of <c>input.ToArray()</c>:
/// <code>
/// var bytes = PayloadBytes.AsArray(input);
/// </code>
/// </example>
public static class PayloadBytes
{
    /// <summary>
    /// The array behind <paramref name="payload"/> when it spans that whole array; otherwise a copy.
    /// </summary>
    /// <param name="payload">The payload.</param>
    /// <returns>An array holding exactly the payload's bytes. Treat it as read-only.</returns>
    public static byte[] AsArray(ReadOnlyMemory<byte> payload) =>
        MemoryMarshal.TryGetArray(payload, out var segment)
        && segment.Array is { } array
        && segment.Offset == 0
        && segment.Count == array.Length
            ? array
            : payload.ToArray();
}
