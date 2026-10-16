using System.Globalization;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Helpers;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;

/// <summary>
/// Moves a payload too large for the broker into the claim store, leaving the item carrying a pointer
/// to it instead (the claim check pattern).
/// </summary>
/// <remarks>
/// <para>
/// The step is total: below the threshold it returns the item untouched, so it needs no conditional
/// execution from the pipeline. It runs before the enqueuer, and the enqueuer's envelope then carries
/// <c>dataref</c> and no data — the order the whole mechanism depends on. Writing the payload before
/// publishing the pointer means a crash can leave an orphaned payload, never a pointer to a payload
/// that was never written.
/// </para>
/// <para>
/// The key is unique per message — <c>{component}/{yyyy}/{MM}/{dd}/{messageId}</c> — and the step
/// mints the message id so the key names the message it travels on: given a dead-lettered message,
/// its payload is findable. The date segments partition the store for the lifecycle policy that
/// expires payloads, which is the platform's to own; the framework never deletes.
/// </para>
/// </remarks>
/// <typeparam name="TCtx">The type of the context used in the step.</typeparam>
/// <param name="store">The claim store the payload is written to. Written through its configured base
/// path, never a per-write override: <see cref="IFileAdapter.GetContentAsync"/> has no equivalent
/// override, so a payload written elsewhere could not be read back. Never listed and never
/// deleted — expiring payloads is a storage lifecycle policy, owned by the platform.</param>
/// <param name="policy">When a payload is claim-checked.</param>
/// <param name="options">The framework options, for the component name in the key and the envelope.</param>
/// <param name="timeProvider">Supplies the date the key is partitioned by. Defaults to
/// <see cref="TimeProvider.System"/>.</param>
public sealed class ClaimCheckStep<TCtx>(
    IFileAdapter store,
    ClaimCheckPolicy policy,
    FrameworkOptions options,
    TimeProvider? timeProvider = null) : TechnicalStep<SourceItem, SourceItem, TCtx>
    where TCtx : Context
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <inheritdoc />
    public override string StepName => "ClaimCheck";

    /// <inheritdoc />
    public override async Task<(TechnicalStepResult<SourceItem> Result, TCtx Context)> ExecuteAsync(
        SourceItem input, TCtx context, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(context);

        // The id names the key, so it is minted here and carried to the envelope: the enqueuer must
        // publish the same id the payload was filed under.
        var item = input.MessageId is { Length: > 0 }
            ? input
            : input with { MessageId = Guid.NewGuid().ToString() };

        var activity = System.Diagnostics.Activity.Current;
        activity?.SetTag("intropy.claim_check.payload_size", item.Data.Length);

        var envelope = CloudEventHelper.Envelope(item, context, options);
        if (!policy.WouldExceed(envelope, item.Data.Length, binary: true))
        {
            // Small enough to ride inside the envelope: the item passes through unchanged.
            activity?.SetTag("intropy.claim_check", false);
            return (new TechnicalStepResult<SourceItem>.Success(input), context);
        }

        var key = KeyFor(item.MessageId!);
        activity?.SetTag("intropy.claim_check", true);
        activity?.SetTag("intropy.claim_check.key", key);

        await store.WriteAsync(key, item.Data, basePathOverride: null, ct);

        // Data is cleared as well as pointed at: the envelope omits it either way, but an empty
        // payload fails small if that branch is ever wrong, rather than publishing the oversized
        // message this whole mechanism exists to prevent.
        return (new TechnicalStepResult<SourceItem>.Success(item with { DataRef = key, Data = [] }), context);
    }

    /// <summary>The claim key for <paramref name="messageId"/>: unique per message, partitioned by the
    /// component and the date so a lifecycle policy can expire it.</summary>
    private string KeyFor(string messageId)
    {
        var now = _time.GetUtcNow();
        return string.Create(CultureInfo.InvariantCulture,
            $"{options.ComponentName}/{now:yyyy}/{now:MM}/{now:dd}/{messageId}");
    }
}
