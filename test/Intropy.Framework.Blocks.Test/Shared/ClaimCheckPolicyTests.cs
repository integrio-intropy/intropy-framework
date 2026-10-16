using System.Reflection;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Blocks.Test.Shared;

/// <summary>
/// The claim-check decision: measuring what the envelope would weigh with its payload inside, without
/// ever encoding the payload to find out.
/// </summary>
public class ClaimCheckPolicyTests
{
    private static CloudEvent Envelope() => new()
    {
        Id = Guid.NewGuid().ToString(),
        Type = "io.intropy.received",
        Source = new Uri("urn:orders"),
        DataContentType = "application/octet-stream"
    };

    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 4)]
    [InlineData(2, 4)]
    [InlineData(3, 4)]
    [InlineData(4, 8)]
    [InlineData(6, 8)]
    [InlineData(7, 12)]
    public void Base64Length_MatchesTheEncoding(int byteCount, int expected)
    {
        Assert.Equal(expected, ClaimCheckPolicy.Base64Length(byteCount));
        Assert.Equal(Convert.ToBase64String(new byte[byteCount]).Length, ClaimCheckPolicy.Base64Length(byteCount));
    }

    [Fact]
    public void EncodedSize_PredictsTheRealEncoding_WithoutEncodingThePayload()
    {
        // The prediction must track the real encoding closely enough that a threshold set below the
        // broker's limit is safe. It is deliberately a touch under: the empty envelope carries no data
        // member at all, so the member's name and framing are not counted.
        var payload = new byte[64 * 1024];
        Array.Fill(payload, (byte)0xA7);
        var envelope = Envelope();

        var predicted = ClaimCheckPolicy.EncodedSize(envelope, payload.Length, binary: true);

        envelope.Data = payload;
        var actual = CloudEventFormat.Formatter.EncodeStructuredModeMessage(envelope, out _).Length;
        Assert.InRange(actual - predicted, 0, 32);
    }

    [Fact]
    public void WouldExceed_IsFalseBelowTheThreshold_AndTrueAbove()
    {
        var policy = ClaimCheckPolicy.Above(4096);

        Assert.False(policy.WouldExceed(Envelope(), payloadLength: 16, binary: true));
        Assert.True(policy.WouldExceed(Envelope(), payloadLength: 8192, binary: true));
    }

    [Fact]
    public void WouldExceed_CountsBase64Growth_SoAPayloadUnderTheThresholdCanStillExceedIt()
    {
        // 3 KiB of bytes is 4 KiB of base64: the threshold is about the message, not the payload.
        var policy = ClaimCheckPolicy.Above(4096);

        Assert.True(policy.WouldExceed(Envelope(), payloadLength: 3 * 1024, binary: true));
        Assert.False(policy.WouldExceed(Envelope(), payloadLength: 3 * 1024, binary: false));
    }

    [Fact]
    public void WouldExceed_IsAlwaysTrue_InAlwaysMode()
    {
        Assert.True(ClaimCheckPolicy.Always.WouldExceed(Envelope(), payloadLength: 0, binary: true));
    }

    [Fact]
    public void TheFactoriesAreTheOnlyWayIn_SoAModeAndThresholdCannotDisagree()
    {
        // The invariant the type rests on: a public constructor would let a caller build
        // Never (which claim-checks nothing, so has no policy) or Auto with a zero threshold
        // (which bypasses the guard in Above). Both once compiled; neither should again.
        Assert.Empty(typeof(ClaimCheckPolicy).GetConstructors(BindingFlags.Public | BindingFlags.Instance));
    }

    [Fact]
    public void Above_RejectsANonPositiveThreshold()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ClaimCheckPolicy.Above(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => ClaimCheckPolicy.Above(-1));
    }
}
