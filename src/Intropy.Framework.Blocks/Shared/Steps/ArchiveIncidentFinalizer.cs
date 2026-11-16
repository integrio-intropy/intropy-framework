using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Core.Pipeline.Abstractions.Enums;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Shared.Steps;

/// <summary>
/// Makes sure the payload of an item that raised a business incident is in the archive, and names
/// the copy on the incident. Runs before the incident is routed.
/// </summary>
/// <remarks>
/// <para>
/// A copy written on receipt (<see cref="ArchiveContextKeys.Ref"/> on the context) is reused;
/// otherwise the payload is written now. Every business failure is archived, retriable ones too: the
/// key is stable across redeliveries, so a redelivered message overwrites its own copy.
/// </para>
/// <para>
/// The write is required. One that fails turns the result into a technical failure, so no incident is
/// raised and the item is not completed: a swept file stays for the next run, and a message is
/// redelivered. A payload is never let go of after an incident without its copy.
/// </para>
/// <para>
/// The incident gets the key as <see cref="ArchiveContextKeys.IncidentDataRef"/> and, with a browse
/// base URL, the copy's URL as <see cref="ArchiveContextKeys.IncidentDataUrl"/>. Its own context is
/// copied, not changed.
/// </para>
/// </remarks>
/// <param name="policy">The archive.</param>
/// <param name="payload">The bytes the pipeline received. A finalizer only sees the result of the
/// step before it, which after a business failure has no value, so the pipeline hands it the input
/// it started with.</param>
/// <typeparam name="T">The type of the pipeline's result.</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
internal sealed class ArchiveIncidentFinalizer<T, TCtx>(ArchivePolicy policy, ReadOnlyMemory<byte> payload)
    : Finalizer<T, TCtx> where TCtx : Context
{
    public override string FinalizerName => "ArchiveIncident";

    public override FinalizerTrigger Triggers => FinalizerTrigger.OnBusinessFailure;

    public override async Task<(StepResult<T> Result, TCtx Context)> ExecuteAsync(StepResult<T> result,
        TCtx context, CancellationToken ct)
    {
        if (result is not StepResult<T>.BusinessFailure failure)
            return (result, context);

        string key;
        if (context.Metadata.TryGetValue(ArchiveContextKeys.Ref, out var archived) && archived.Length > 0)
        {
            key = archived;
            ArchiveWriter.Tag(System.Diagnostics.Activity.Current, policy, key);
        }
        else
        {
            key = await ArchiveWriter.WriteAsync(policy, context, payload, ct);
        }

        var incidentContext = failure.Value.Context is { } existing
            ? new Dictionary<string, string>(existing)
            : [];
        incidentContext[ArchiveContextKeys.IncidentDataRef] = key;
        if (policy.UrlOf(key) is { } url)
            incidentContext[ArchiveContextKeys.IncidentDataUrl] = url;

        var incident = new BusinessIncidentData { Description = failure.Value.Description, Context = incidentContext };
        return (failure with { Value = incident }, context);
    }
}
