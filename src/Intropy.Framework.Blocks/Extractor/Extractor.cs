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
///   <item>Deserialize: deserializes the string data into a POCO</item>
///   <item>Extractor(s): Extract additional information from external sources</item>     
///   <item>Validate: validates the POCO</item>
///   <item>Idempotency check: checks so that the same data has not been processed already</item>
///   <item>Transform: transforms the input POCO into an output POCO</item>
///   <item>Serialize: serializes the output POCO into string</item>
///   <item>Send: sends the string data to the destination</item>
///   <item>Record idempotency: records that the data has been processed</item>
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
/// <param name="idempotencyChecker"></param>
/// <param name="idempotencyRecorder"></param>
/// <param name="businessIncidentRouter"></param>
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
    IdempotencyCheckStep<TInput, TCtx> idempotencyChecker,
    IdempotencyRecordStep<CloudEvent, TCtx> idempotencyRecorder,
    BusinessIncidentRouteStep<CloudEvent, TCtx> businessIncidentRouter)
    where TCtx : Context
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="input">The input data to use</param>
    /// <param name="context">The pipeline context</param>
    /// <param name="detachTrace">Indicate whether to detach the trace or continue</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution</param>
    /// <returns></returns>
    public async Task<(StepResult<CloudEvent> Result, TCtx Context)> Execute(string input, TCtx context, bool detachTrace = true,
        CancellationToken ct = default)
    {
        return await PipelineTracing.ExecuteWithTracing(async () => await Pipeline
                .Start(input, context, ct)
                .AddStep(deserializer)
                .AddStep(validator)
                .AddSteps(extractors)
                .AddStep(idempotencyChecker)
                .AddStep(transformer)
                .AddStep(serializer)
                .AddStep(sender)
                .AddStep(idempotencyRecorder)
                .AddFinalizer(businessIncidentRouter),
            pipelineName: pipelineName,
            logger: logger,
            configureActivity: _ => { },
            detachTrace: detachTrace
        );
    }
}
