using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.TransactionalIntegration;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

/// <summary>
/// The claim-check settings on a Transactional Integration's options. The threshold is the broker's
/// maximum message size, which the framework cannot know, so it is required rather than defaulted:
/// a wrong default fails at publish, which is the failure the claim check exists to prevent.
/// </summary>
public class ClaimCheckOptionsTests
{
    [Fact]
    public void ClaimCheckPolicy_IsTheConfiguredThreshold_InAutoMode()
    {
        var options = new TransactionalIntegrationOptions { ClaimCheckThresholdBytes = 262_144 };

        var policy = options.ClaimCheckPolicy();

        Assert.NotNull(policy);
        Assert.Equal(ClaimCheckMode.Auto, policy.Mode);
        Assert.Equal(262_144, policy.ThresholdBytes);
    }

    [Fact]
    public void ClaimCheckPolicy_NeedsNoThreshold_InAlwaysMode()
    {
        var options = new TransactionalIntegrationOptions { ClaimCheckMode = ClaimCheckMode.Always };

        var policy = options.ClaimCheckPolicy();

        Assert.NotNull(policy);
        Assert.Equal(ClaimCheckMode.Always, policy.Mode);
    }

    [Fact]
    public void ClaimCheckPolicy_IsNothingAtAll_InNeverMode()
    {
        // No policy means no step: the off switch costs nothing per message, while the store stays
        // registered so claims already on the queue still resolve.
        var options = new TransactionalIntegrationOptions { ClaimCheckMode = ClaimCheckMode.Never };

        Assert.Null(options.ClaimCheckPolicy());
    }

    [Fact]
    public void ClaimCheckPolicy_NeedsNoThreshold_InNeverMode()
    {
        // Rolling back must not require supplying a limit for something that will not be measured.
        var options = new TransactionalIntegrationOptions
        {
            ClaimCheckMode = ClaimCheckMode.Never,
            ClaimCheckThresholdBytes = null
        };

        Assert.Null(Record.Exception(() => options.ClaimCheckPolicy()));
    }

    [Fact]
    public void ClaimCheckPolicy_SaysWhichSettingIsMissing_WhenAutoHasNoThreshold()
    {
        var options = new TransactionalIntegrationOptions();

        var error = Assert.Throws<InvalidOperationException>(() => options.ClaimCheckPolicy());

        Assert.Contains(nameof(TransactionalIntegrationOptions.ClaimCheckThresholdBytes), error.Message,
            StringComparison.Ordinal);
    }
}
