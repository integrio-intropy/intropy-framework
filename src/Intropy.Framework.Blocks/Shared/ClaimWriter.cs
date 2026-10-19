using System.Diagnostics;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Common;
using Intropy.Framework.Core.Configuration;

namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Files a claim-checked payload in the claim store: the part of the claim check that is the same
/// wherever a payload is produced.
/// </summary>
/// <remarks>
/// <para>
/// Two blocks claim-check payloads — a Transactional Integration before it enqueues, an extractor
/// before it publishes — and the cross-component contract they produce is one contract. Keeping the
/// writing sequence here is what stops them drifting: the id validation below reached only one of
/// them when it was first written, which is exactly the failure this exists to prevent.
/// </para>
/// <para>
/// It owns only what both sides genuinely share. Deciding <em>whether</em> to claim-check is not
/// shared — the two measure different things, a raw length against a base64 factor on one side and
/// an already-encoded length on the other — and neither is pointing the message at the key, which
/// one does on the envelope and the other on a record the enqueuer reads later.
/// </para>
/// </remarks>
internal static class ClaimWriter
{
    private const string ClaimCheckTag = "intropy.claim_check";
    private const string PayloadSizeTag = "intropy.claim_check.payload_size";
    private const string KeyTag = "intropy.claim_check.key";

    /// <summary>The span covering the store write. Named for the mechanism rather than the step, so
    /// one query finds it in every block.</summary>
    private const string ClaimCheckSpan = "ClaimCheck";

    /// <summary>
    /// Refuses a component name or message id the key format cannot hold, so a caller can bring that
    /// refusal forward to every payload rather than letting it wait for the first oversized one.
    /// </summary>
    /// <remarks>
    /// <see cref="WriteAsync"/> checks the same things, so calling this is never required for
    /// correctness — it only decides <em>when</em> an unusable configuration is reported. Doing it
    /// before the size decision is what turns "this component fails weeks from now, the first time a
    /// payload grows" into "this component fails on its first payload".
    /// </remarks>
    /// <param name="options">The framework options, for the component name.</param>
    /// <param name="messageId">The id of the message a pointer would travel on.</param>
    /// <exception cref="InvalidOperationException">Either cannot name part of a claim key.</exception>
    internal static void RequireCanName(FrameworkOptions options, string? messageId)
    {
        RequireComponentCanName(options);
        Require(messageId, "message id",
            "Give the message an id of that shape — the GUIDs the framework mints already are — or turn " +
            "the claim check off for this component.");
    }

    /// <summary>
    /// Refuses a component name the key format cannot hold, for a caller configuring a claim check
    /// before it has any payload to file.
    /// </summary>
    /// <remarks>
    /// The component name is the one key segment that is deployment configuration rather than
    /// something the framework mints, which makes it the likelier of the two to arrive unusable —
    /// and the one worth catching when the pipeline is composed, because by then it is already known
    /// and cannot change. Checking it there turns a run that fails on its first oversized payload
    /// into one that never starts.
    /// </remarks>
    /// <param name="options">The framework options, for the component name.</param>
    /// <exception cref="InvalidOperationException">The component name cannot name part of a claim
    /// key.</exception>
    internal static void RequireComponentCanName(FrameworkOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Require(options.ComponentName, "component name",
            $"Set {nameof(FrameworkOptions)}.{nameof(FrameworkOptions.ComponentName)} to a name of that shape.");
    }

    /// <summary>Records that a payload of <paramref name="payloadSize"/> bytes stayed inside its
    /// message, so a trace says so rather than being silent about it.</summary>
    /// <param name="payloadSize">The payload's size in bytes.</param>
    internal static void Inline(int payloadSize)
    {
        var activity = Activity.Current;
        activity?.SetTag(PayloadSizeTag, payloadSize);
        activity?.SetTag(ClaimCheckTag, false);
    }

    /// <summary>
    /// Writes <paramref name="payload"/> to the claim store under a key naming
    /// <paramref name="messageId"/>, and returns that key for the message to point at.
    /// </summary>
    /// <remarks>
    /// The payload is written before anything publishes a pointer to it, so a crash can leave an
    /// orphaned payload but never a pointer to a payload that was never written. The framework never
    /// deletes: the date segments partition the store for the lifecycle policy that expires
    /// payloads, which is the platform's to own.
    /// </remarks>
    /// <param name="store">The claim store. Written through its configured base path, never a
    /// per-write override: <see cref="IFileAdapter.GetContentAsync"/> has no equivalent override, so
    /// a payload written elsewhere could not be read back.</param>
    /// <param name="options">The framework options, for the component name in the key.</param>
    /// <param name="time">Supplies the date the key is partitioned by.</param>
    /// <param name="messageId">The id of the message the pointer will travel on.</param>
    /// <param name="payload">The bytes a consumer must receive.</param>
    /// <param name="ct">A token that aborts the write.</param>
    /// <returns>The claim key.</returns>
    /// <exception cref="InvalidOperationException">The component name or the message id cannot name
    /// part of a claim key. Checked here, by name, rather than left to the
    /// <see cref="ArgumentException"/> from inside <see cref="ClaimKey.For"/>, because of when it
    /// would otherwise surface: a component with an unusable id scheme publishes small payloads
    /// perfectly well and only fails the first time one crosses the threshold.</exception>
    internal static async Task<string> WriteAsync(IFileAdapter store, FrameworkOptions options,
        TimeProvider time, string messageId, byte[] payload, CancellationToken ct)
    {
        RequireCanName(options, messageId);

        var key = ClaimKey.For(options.ComponentName, time.GetUtcNow(), messageId);

        // On the step's own span, so "which messages were claim-checked?" is one query whichever
        // block produced them — the step span is the only one both blocks reliably share.
        var step = Activity.Current;
        step?.SetTag(PayloadSizeTag, payload.Length);
        step?.SetTag(ClaimCheckTag, true);

        // The write gets a span of its own because it is I/O that can be slow or fail, and without
        // one it hides inside the enqueue or send it precedes — leaving a slow claim store
        // indistinguishable from a slow broker, which is the thing that costs an incident.
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity(ClaimCheckSpan);
        activity?.SetTag(KeyTag, key);
        activity?.SetTag(PayloadSizeTag, payload.Length);
        // Also on the step span, so a trace that samples away the child still names the payload.
        step?.SetTag(KeyTag, key);

        try
        {
            await store.WriteAsync(key, payload, basePathOverride: null, ct);
        }
        catch (Exception e)
        {
            activity?.SetStatus(ActivityStatusCode.Error, e.Message);
            throw;
        }

        return key;
    }

    /// <summary>Refuses a value the key format cannot hold, naming the value and what to do.</summary>
    private static void Require(string? value, string what, string remedy)
    {
        if (!ClaimKey.CanName(value))
            throw new InvalidOperationException(
                $"The {what} '{value}' cannot be claim-checked: it names the payload's key in the claim store, " +
                "so it must be one or more letters, digits, '.', '-' or '_', and neither '.' nor '..'. " +
                remedy);
    }
}
