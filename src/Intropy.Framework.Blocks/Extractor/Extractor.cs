using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.Extractor;

/// <summary>
/// A pipeline template used for extractor components. It follows these steps:
/// <list type="number">
///   <item>Archive (optional, archive mode OnReceipt): copies the received payload to the archive store</item>
///   <item>Deserialize: deserializes the raw bytes into a POCO</item>
///   <item>Validate: validates the POCO</item>
///   <item>Extractor(s): Extract additional information from external sources</item>
///   <item>Idempotency check (optional): checks so that the same data has not been processed already</item>
///   <item>Transform: transforms the input POCO into an output POCO</item>
///   <item>Serialize: serializes the output POCO into string</item>
///   <item>Send: sends the string data to the destination</item>
///   <item>Record idempotency (optional): records that the data has been processed</item>
///   <item>Archive incident (optional): makes sure an incident's payload is archived, and names the copy on the incident</item>
///   <item>Route business incidents: routes any business incidents that occurred during the execution</item>
/// </list>
/// </summary>
/// <param name="pipelineName"></param>
/// <param name="logger"></param>
/// <param name="deserializer"></param>
/// <param name="extractors"></param>
/// <param name="validator"></param>
/// <param name="transformer"></param>
/// <param name="serializer"></param>
/// <param name="sender"></param>
/// <param name="idempotencyChecker">Optional idempotency check step. Skipped when null.</param>
/// <param name="idempotencyRecorder">Optional idempotency record step. Skipped when null.</param>
/// <param name="businessIncidentRouter"></param>
/// <param name="archive">Optional archive the received payload is copied to: on receipt, on a
/// business incident, or both (see <see cref="ArchiveMode"/>). Skipped when null.</param>
/// <typeparam name="TInput"></typeparam>
/// <typeparam name="TOutput"></typeparam>
/// <typeparam name="TCtx"></typeparam>
public class Extractor<TInput, TOutput, TCtx>(
    string pipelineName,
    ILogger logger,
    DeserializeStep<TInput, TCtx> deserializer,
    IReadOnlyList<ExtractStep<TInput, TCtx>> extractors,
    ValidateStep<TInput, TCtx> validator,
    TransformStep<TInput, TOutput, TCtx> transformer,
    SerializeStep<TOutput, TCtx> serializer,
    SendStep<TCtx> sender,
    IdempotencyCheckStep<TInput, TCtx>? idempotencyChecker,
    IdempotencyRecordStep<CloudEvent, TCtx>? idempotencyRecorder,
    BusinessIncidentRouteStep<CloudEvent, TCtx> businessIncidentRouter,
    ArchivePolicy? archive = null)
    where TCtx : Context
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="input">The raw bytes of the source item, passed to the deserializer undecoded</param>
    /// <param name="context">The pipeline context</param>
    /// <param name="detachTrace">When false (the default), the execution continues the ambient
    /// trace — what a host sets up per item (the file's or message's own trace); when true, it
    /// starts its own trace, linked to the current one. Hosts that prepare an item trace pass
    /// false explicitly; standalone callers rarely need to detach.</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution</param>
    /// <returns></returns>
    public async Task<(StepResult<CloudEvent> Result, TCtx Context)> Execute(ReadOnlyMemory<byte> input, TCtx context, bool detachTrace = false,
        CancellationToken ct = default)
    {
        return await PipelineTracing.ExecuteWithTracing(async () => await Pipeline
                .Start(input, context, ct)
                .AddOptionalStep(ArchiveSteps.OnReceipt<TCtx>(archive, logger))
                .AddStep(deserializer)
                .AddStep(validator)
                .AddSteps(extractors)
                .AddOptionalStep(idempotencyChecker)
                .AddStep(transformer)
                .AddStep(serializer)
                .AddStep(sender)
                .AddOptionalStep(idempotencyRecorder)
                .AddOptionalFinalizer(ArchiveSteps.OnIncident<CloudEvent, TCtx>(archive, input))
                .AddFinalizer(businessIncidentRouter),
            pipelineName: pipelineName,
            logger: logger,
            configureActivity: _ => { },
            detachTrace: detachTrace
        );
    }
}
