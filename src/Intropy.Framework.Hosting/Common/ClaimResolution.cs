using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Common;

/// <summary>
/// Resolves a message's payload: the claim check's consuming half, shared by every route that
/// consumes from a topic.
/// </summary>
/// <remarks>
/// It lives here, rather than on either route, because the two sides of the claim check are a
/// cross-component contract: a Transactional Integration resolves pointers off its own internal
/// queue, while a loader resolves pointers published by <em>other</em> components onto the system
/// bus. One implementation is what keeps the validation and the failure messages identical on both,
/// which matters most on the loader, where the publisher is not the same deployment.
/// </remarks>
internal static class ClaimResolution
{
    /// <summary>
    /// The message's payload: fetched from the claim store when the message carries a pointer instead
    /// of its payload, and the message's own data otherwise.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Resolving here, rather than in a pipeline step, keeps the claim check invisible to the
    /// pipeline: its input is the payload either way, so no component's code knows the mechanism
    /// exists. A failed fetch throws and becomes a technical failure, so the message is redelivered
    /// and eventually dead-lettered — never reported as a business failure.
    /// </para>
    /// <para>
    /// This consults no claim-check policy, deliberately: the mode governs whether a component
    /// <em>writes</em> claims, while the registered store governs whether it can resolve them. That is
    /// what lets a rollback stop new claims and still drain the ones already on the queue.
    /// </para>
    /// </remarks>
    /// <param name="message">The message as the sidecar delivered it.</param>
    /// <param name="scope">The message's own service scope, which the claim store is resolved from.</param>
    /// <param name="cancellationToken">A token that aborts the fetch.</param>
    /// <returns>The payload the pipeline runs on.</returns>
    /// <exception cref="InvalidOperationException">The pointer is not a claim key, or the message
    /// carries one but the component has no claim store registered. The latter is the shape of a
    /// mis-ordered rollout — a publisher claim-checking before its consumers can resolve — so it says
    /// exactly that rather than surfacing later as an empty payload.</exception>
    internal static async Task<ReadOnlyMemory<byte>> ResolveAsync(IncomingMessage message,
        IServiceProvider scope, CancellationToken cancellationToken)
    {
        if (message.GetExtension(CloudEventExtensionKeys.DataRef) is not { Length: > 0 } dataRef)
            return message.Data;

        // Checked before the store is even resolved: a pointer the publishing side cannot have minted
        // is rejected as malformed whether or not a store is registered. The adapters join a key onto
        // the store's base path without normalising it, so an unchecked pointer would name any file
        // the binding can reach — see ClaimKey.
        if (!ClaimKey.IsValid(dataRef))
            throw new InvalidOperationException(
                $"Message '{message.MessageId}' carries a claim pointer ('{dataRef}') that is not a claim key: " +
                "a key is '{component}/{yyyy}/{MM}/{dd}/{messageId}'. The payload is not fetched; the message " +
                "was not published by this framework's claim check, or it was tampered with in transit.");

        var store = scope.GetKeyedService<IFileAdapter>(ClaimStoreServiceCollectionExtensions.ClaimStoreKey)
            ?? throw new InvalidOperationException(
                $"Message '{message.MessageId}' carries a claim pointer ('{dataRef}') but no claim store is " +
                "registered: its payload is in a store this component cannot read. Register one with " +
                "AddClaimStore(configuration), configured against the same store the publisher writes to.");

        return await store.GetContentAsync(dataRef, cancellationToken);
    }
}
