using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.Configuration;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>
/// The extractor's own settings, separate from the <see cref="Jobs.JobOptions"/> that govern how its
/// job runs.
/// </summary>
/// <remarks>
/// These are deployment decisions rather than pipeline ones, which is why they are options bound
/// from configuration rather than builder calls: switching the claim check off has to be something
/// an operator can do without a code change. See
/// <see cref="Blocks.Shared.ClaimCheckMode.Never"/> for the rollback that depends on it.
/// </remarks>
public sealed class ExtractorOptions
{
    /// <summary>
    /// When a payload is moved to the claim store instead of riding inside its event. Applies only
    /// when a claim store is registered (<c>AddClaimStore</c>); without one every payload stays
    /// inline, whatever this says.
    /// </summary>
    /// <remarks>
    /// This governs <em>writing</em> claims only. Consumers resolve a pointer whenever an event
    /// carries one, whatever this extractor's mode, so <see cref="Blocks.Shared.ClaimCheckMode.Never"/>
    /// stops new claims while leaving existing ones to drain — see that mode for the rollback order.
    /// </remarks>
    /// <value>Default: <see cref="Blocks.Shared.ClaimCheckMode.Auto"/>, which requires
    /// <see cref="ClaimCheckThresholdBytes"/>.</value>
    public ClaimCheckMode ClaimCheckMode { get; set; } = ClaimCheckMode.Auto;

    /// <summary>
    /// The largest encoded event that may be published inline, in bytes. Required when a claim store
    /// is registered in <see cref="Blocks.Shared.ClaimCheckMode.Auto"/>, and ignored in
    /// <see cref="Blocks.Shared.ClaimCheckMode.Always"/> and
    /// <see cref="Blocks.Shared.ClaimCheckMode.Never"/>.
    /// </summary>
    /// <remarks>
    /// Set it 1–2 KB below the broker's maximum message size rather than at it. The measurement is
    /// taken before the trace context is attached to the envelope, and Dapr adds attributes of its own
    /// (<c>topic</c>, <c>pubsubname</c>, <c>traceid</c>) when it forwards the event, so the message the
    /// broker is handed is always somewhat larger than what the threshold was compared against.
    /// </remarks>
    public int? ClaimCheckThresholdBytes { get; set; }

    /// <summary>
    /// Applies <paramref name="configuration"/>'s values to these options: <c>ClaimCheckMode</c> and
    /// <c>ClaimCheckThresholdBytes</c>.
    /// </summary>
    /// <remarks>
    /// Key by key rather than by binder, so an absent key leaves a code-set value alone and a
    /// malformed one fails naming the member it was meant for. Call it first in
    /// <c>configureOptions</c> to let a deployment supply the values and code override them —
    /// switching the claim check off is a deployment decision, and must not need a rebuild.
    /// </remarks>
    /// <param name="configuration">The section holding the extractor's values.</param>
    /// <returns>These options, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">A key holds a value of the wrong shape.</exception>
    public ExtractorOptions Bind(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        if (ComponentConfigurationReader.Enum<ClaimCheckMode>(configuration, nameof(ClaimCheckMode),
                $"{nameof(ExtractorOptions)}.{nameof(ClaimCheckMode)}") is { } mode)
            ClaimCheckMode = mode;
        if (ComponentConfigurationReader.Int32(configuration, nameof(ClaimCheckThresholdBytes),
                $"{nameof(ExtractorOptions)}.{nameof(ClaimCheckThresholdBytes)}") is { } threshold)
            ClaimCheckThresholdBytes = threshold;
        return this;
    }

    /// <summary>The claim-check policy these options describe, or <see langword="null"/> in
    /// <see cref="Blocks.Shared.ClaimCheckMode.Never"/>: nothing is claim-checked, so the decorator is
    /// left out of the pipeline rather than carried as a no-op.</summary>
    /// <exception cref="InvalidOperationException">The mode needs a threshold and none is set.</exception>
    internal ClaimCheckPolicy? ClaimCheckPolicy() => ClaimCheckMode switch
    {
        ClaimCheckMode.Never => null,
        ClaimCheckMode.Always => Blocks.Shared.ClaimCheckPolicy.Always,
        _ => Blocks.Shared.ClaimCheckPolicy.Above(ClaimCheckThresholdBytes ?? throw new InvalidOperationException(
            $"{nameof(ExtractorOptions)}.{nameof(ClaimCheckThresholdBytes)} must be set when a claim " +
            $"store is registered and {nameof(ClaimCheckMode)} is {ClaimCheckMode.Auto}: it is the " +
            "broker's maximum message size, which the framework cannot know. Set it below that limit with room to " +
            $"spare, or use {nameof(ClaimCheckMode)} {ClaimCheckMode.Always} or " +
            $"{ClaimCheckMode.Never}."))
    };

    /// <summary>Rejects a threshold that could never be met, at registration rather than at the
    /// first oversized payload.</summary>
    /// <remarks>Whether a threshold is <em>required</em> depends on the mode and on whether a store
    /// is registered, so that check stays in <see cref="ClaimCheckPolicy"/> where both are
    /// known.</remarks>
    /// <param name="paramName">The parameter to name in the exception.</param>
    /// <exception cref="ArgumentException">The threshold is set and not positive.</exception>
    internal void EnsureValidClaimCheckThreshold(string paramName)
    {
        if (ClaimCheckThresholdBytes is { } bytes && bytes <= 0)
            throw new ArgumentException(
                $"{nameof(ExtractorOptions)}.{nameof(ClaimCheckThresholdBytes)} must be positive when " +
                $"set; got {bytes}.", paramName);
    }
}
