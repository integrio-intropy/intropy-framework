using Intropy.Framework.Adapters.File;

namespace Intropy.Framework.Blocks.Shared;

/// <summary>When a pipeline copies the payload it received to the archive store.</summary>
/// <remarks>
/// Every mode but <see cref="Never"/> archives the payload of a business incident. Archiving what a
/// component sends is not implemented yet; when it is, it arrives as two new values — <c>OnSend</c>
/// (incidents and every sent payload) and <c>Always</c> (everything received and sent) — so no
/// existing value changes meaning on an upgrade.
/// </remarks>
public enum ArchiveMode
{
    /// <summary>
    /// Only when the item raises a business incident: the payload is written before the incident is
    /// routed, and the incident carries a reference to it. The default, because an incident is the
    /// moment a copy matters — a swept file is completed (deleted) right after.
    /// </summary>
    OnIncident,

    /// <summary>
    /// Every item, as it is received, before any step runs — so an item that cannot even be
    /// deserialized is archived too. Best effort: a failed write is logged and the item is processed
    /// without a copy. An incident raised later reuses the copy, or writes one if the first write
    /// failed.
    /// </summary>
    OnReceipt,

    /// <summary>Nothing is archived, whether or not an archive store is registered.</summary>
    Never
}

/// <summary>
/// The archive a pipeline copies its payloads to, and when.
/// </summary>
/// <remarks>
/// Public only so a pipeline's constructor can take it: there is no public way to create one. The
/// hosting layer creates it from the registered archive store (<c>AddArchiveStore</c>) and the
/// component's <c>Archive</c> configuration, so whether a component archives is a deployment
/// decision, the same as the claim check.
/// </remarks>
public sealed class ArchivePolicy
{
    internal ArchivePolicy(string componentName, IFileAdapter store, ArchiveMode mode, Uri? browseBaseUrl,
        TimeProvider time)
    {
        // The component name is the first segment of every key and is known now, so an unusable one
        // fails when the pipeline is composed rather than on the first payload it archives.
        ArchiveKey.RequireComponentCanName(componentName);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(time);
        if (mode == ArchiveMode.Never)
            throw new ArgumentException("A policy that archives nothing is no policy: leave it out instead.",
                nameof(mode));
        if (browseBaseUrl is { IsAbsoluteUri: false })
            throw new ArgumentException("The browse base URL must be absolute.", nameof(browseBaseUrl));

        ComponentName = componentName;
        Store = store;
        Mode = mode;
        BrowseBaseUrl = browseBaseUrl;
        Time = time;
    }

    /// <summary>The component archiving: the first segment of every key.</summary>
    internal string ComponentName { get; }

    /// <summary>The archive store. Written through its configured base path.</summary>
    internal IFileAdapter Store { get; }

    /// <summary>When payloads are written.</summary>
    internal ArchiveMode Mode { get; }

    /// <summary>Where an archived payload can be opened, the key appended: the storage's own address
    /// for the archive container, so a trace or an incident links straight to the copy. The
    /// framework cannot read it from the binding, so it is configuration; without it, only the key
    /// is given.</summary>
    internal Uri? BrowseBaseUrl { get; }

    /// <summary>Supplies the date a key is partitioned by when the item carries none.</summary>
    internal TimeProvider Time { get; }

    /// <summary>The browse URL of <paramref name="key"/>, or <see langword="null"/> without a
    /// browse base URL.</summary>
    internal string? UrlOf(string key) =>
        BrowseBaseUrl is { } baseUrl ? baseUrl.AbsoluteUri.TrimEnd('/') + "/" + key : null;
}

/// <summary>The archive's steps for a pipeline, so each block adds them the same way.</summary>
internal static class ArchiveSteps
{
    /// <summary>The step that archives on receipt: only in <see cref="ArchiveMode.OnReceipt"/>.</summary>
    internal static Steps.ArchiveReceivedStep<TCtx>? OnReceipt<TCtx>(ArchivePolicy? policy,
        Microsoft.Extensions.Logging.ILogger logger) where TCtx : Context =>
        policy is { Mode: ArchiveMode.OnReceipt } ? new Steps.ArchiveReceivedStep<TCtx>(policy, logger) : null;

    /// <summary>The finalizer that archives an incident's payload, for one execution: it is handed
    /// the input that execution started with.</summary>
    internal static Steps.ArchiveIncidentFinalizer<T, TCtx>? OnIncident<T, TCtx>(ArchivePolicy? policy,
        ReadOnlyMemory<byte> input) where TCtx : Context =>
        policy is null ? null : new Steps.ArchiveIncidentFinalizer<T, TCtx>(policy, input);
}
