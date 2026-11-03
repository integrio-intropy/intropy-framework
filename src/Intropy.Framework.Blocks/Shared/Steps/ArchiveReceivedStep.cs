using System.Diagnostics;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.Shared.Steps;

/// <summary>
/// Copies the payload a pipeline received to the archive store before any step runs, in
/// <see cref="ArchiveMode.OnReceipt"/>. The payload passes through untouched.
/// </summary>
/// <remarks>
/// Best effort: a failed write is logged and recorded on the step's span, and the item is processed
/// without a copy. An archive outage must not stop every integration that archives; if the item later
/// raises an incident, <see cref="ArchiveIncidentFinalizer{T,TCtx}"/> finds no copy and writes one,
/// and that write is required.
/// </remarks>
/// <typeparam name="TCtx">The type of the context.</typeparam>
internal sealed class ArchiveReceivedStep<TCtx>(ArchivePolicy policy, ILogger logger)
    : TechnicalStep<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>, TCtx> where TCtx : Context
{
    public override string StepName => "ArchiveReceived";

    public override async Task<(TechnicalStepResult<ReadOnlyMemory<byte>> Result, TCtx Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, TCtx context, CancellationToken ct)
    {
        try
        {
            await ArchiveWriter.WriteAsync(policy, context, input, ct);
        }
        catch (Exception e) when (!ct.IsCancellationRequested)
        {
            Activity.Current?.AddException(e);
            logger.LogWarning(e, "Could not archive the received payload; processing it without a copy");
        }

        return (new TechnicalStepResult<ReadOnlyMemory<byte>>.Success(input), context);
    }
}
