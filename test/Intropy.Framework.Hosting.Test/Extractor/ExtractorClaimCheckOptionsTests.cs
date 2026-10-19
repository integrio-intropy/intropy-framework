using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Extractor;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// What the extractor's claim-check options describe: a policy, or nothing at all. The mode decides
/// whether a threshold is needed, so the two cannot be validated apart.
/// </summary>
public class ExtractorClaimCheckOptionsTests
{
    [Fact]
    public void ClaimCheckPolicy_IsTheConfiguredThreshold_InAutoMode()
    {
        var options = new ExtractorOptions { ClaimCheckMode = ClaimCheckMode.Auto, ClaimCheckThresholdBytes = 230_000 };

        var policy = options.ClaimCheckPolicy();

        Assert.Equal(ClaimCheckMode.Auto, policy!.Mode);
        Assert.Equal(230_000, policy.ThresholdBytes);
    }

    [Fact]
    public void ClaimCheckPolicy_NeedsNoThreshold_InAlwaysMode()
    {
        var options = new ExtractorOptions { ClaimCheckMode = ClaimCheckMode.Always };

        Assert.Equal(ClaimCheckMode.Always, options.ClaimCheckPolicy()!.Mode);
    }

    // Never means no decorator at all, rather than one carried as a no-op.
    [Fact]
    public void ClaimCheckPolicy_IsNothingAtAll_InNeverMode()
    {
        var options = new ExtractorOptions { ClaimCheckMode = ClaimCheckMode.Never };

        Assert.Null(options.ClaimCheckPolicy());
    }

    [Fact]
    public void ClaimCheckPolicy_SaysWhichSettingIsMissing_WhenAutoHasNoThreshold()
    {
        var options = new ExtractorOptions { ClaimCheckMode = ClaimCheckMode.Auto };

        var error = Assert.Throws<InvalidOperationException>(() => options.ClaimCheckPolicy());

        Assert.Contains(nameof(ExtractorOptions.ClaimCheckThresholdBytes), error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EnsureValidClaimCheckThreshold_RejectsAThresholdThatCouldNeverBeMet()
    {
        var options = new ExtractorOptions { ClaimCheckThresholdBytes = 0 };

        Assert.Throws<ArgumentException>(() => options.EnsureValidClaimCheckThreshold("options"));
    }

    [Fact]
    public void EnsureValidClaimCheckThreshold_AcceptsNoThresholdAtAll()
    {
        new ExtractorOptions().EnsureValidClaimCheckThreshold("options");
    }
}
