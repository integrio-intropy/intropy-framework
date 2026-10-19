using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Helpers;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;

/// <summary>
/// Moves a payload too large for the broker into the claim store, leaving the item carrying a
/// pointer to it instead (the claim check pattern), then enqueues it with the configured enqueuer.
/// </summary>
/// <remarks>
/// <para>
/// It decorates the enqueuer rather than sitting in the pipeline ahead of it, matching
/// <see cref="Extractor.Steps.ClaimCheckSend{TCtx}"/> on the publishing side: one concept, one
/// shape, wherever a payload is claim-checked. It is total — below the threshold the item is
/// enqueued untouched — so it needs no conditional composition.
/// </para>
/// <para>
/// It has to run before <see cref="EnqueueStep{TCtx}"/> builds the envelope, because by the time
/// the envelope exists the oversized message has already been materialised. That is why it overrides
/// the item-level <c>ExecuteAsync</c> and delegates, rather than implementing the <c>cloudEvent</c>
/// overload every other enqueuer implements.
/// </para>
/// <para>
/// The payload is written before the pointer is published, so a crash can leave an orphaned payload
/// but never a pointer to a payload that was never written.
/// </para>
/// </remarks>
/// <typeparam name="TCtx">The type of the context used in the step.</typeparam>
/// <param name="inner">The enqueuer the item is handed to once its payload has been settled.</param>
/// <param name="store">The claim store the payload is written to.</param>
/// <param name="policy">When a payload is claim-checked.</param>
/// <param name="options">The framework options, for the component name in the key and the envelope.</param>
/// <param name="timeProvider">Supplies the date the key is partitioned by. Defaults to
/// <see cref="TimeProvider.System"/>.</param>
internal sealed class ClaimCheckEnqueue<TCtx>(
    EnqueueStep<TCtx> inner,
    IFileAdapter store,
    ClaimCheckPolicy policy,
    FrameworkOptions options,
    TimeProvider? timeProvider = null) : EnqueueStep<TCtx>(options)
    where TCtx : Context
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    // Held rather than read off the primary-ctor parameter, which the base class also captures.
    private readonly FrameworkOptions _options = options;

    /// <inheritdoc />
    /// <remarks>The enqueuer's own, so the pipeline reports one step and the claim check's own work
    /// is attributable through the span <see cref="ClaimWriter"/> starts around the store write
    /// rather than by renaming the step the item is actually in.</remarks>
    public override string StepName => inner.StepName;

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

        // Checked before the size decision below, so every item is checked rather than only the ones
        // large enough to be claim-checked. An id the key format cannot hold reaches here only from
        // component code supplying its own SourceItem — the framework's own sweep never sets one —
        // but when it does, failing on the first item beats failing on the first large one.
        ClaimWriter.RequireCanName(_options, item.MessageId);

        // The envelope is built without the payload: what the broker rejects is the whole message, so
        // the threshold is measured against the attributes too. The raw length goes in and the policy
        // applies the base64 factor, because a byte[] payload always encodes as data_base64.
        var envelope = CloudEventHelper.Envelope(item, context, _options);
        if (!policy.WouldExceed(envelope, item.Data.Length, binary: true))
        {
            // Small enough to ride inside the envelope: the item is enqueued unchanged.
            ClaimWriter.Inline(item.Data.Length);
            return await inner.ExecuteAsync(input, context, ct);
        }

        var key = await ClaimWriter.WriteAsync(store, _options, _time, item.MessageId!, item.Data, ct);

        // Data is cleared as well as pointed at: the envelope omits it either way, but an empty
        // payload fails small if that branch is ever wrong, rather than publishing the oversized
        // message this whole mechanism exists to prevent.
        return await inner.ExecuteAsync(item with { DataRef = key, Data = [] }, context, ct);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Never reached through this decorator, which delegates at the item level above and so leaves
    /// the envelope to the wrapped enqueuer's own body. Forwarded rather than thrown so the type
    /// is a true stand-in for what it wraps.
    /// </remarks>
    public override Task<(TechnicalStepResult<SourceItem> Result, TCtx Context)> ExecuteAsync(
        SourceItem input, ReadOnlyMemory<byte> cloudEvent, TCtx context, CancellationToken ct) =>
        inner.ExecuteAsync(input, cloudEvent, context, ct);
}
