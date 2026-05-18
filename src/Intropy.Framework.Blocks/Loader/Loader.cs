using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.Loader;

/// <summary>
/// A pipeline template used for loader components. It follows these steps:
/// <list type="number">
///   <item>Deserialize: deserializes the CloudEvent data into a POCO, extracts CloudEvent metadata to context</item>
///   <item>Extract: extracts data from the input</item>
///   <item>Validate: validates the POCO</item>
///   <item>Idempotency check: checks so that the same data has not been processed already</item>
///   <item>Transform: transforms the input POCO into an output POCO</item>
///   <item>Send: sends the data to the external system</item>
///   <item>Record idempotency: records that the data has been processed</item>
///   <item>Send receipt (optional): sends a receipt after a successful load</item>
///   <item>Route business incidents: routes any business incidents that occurred during the execution</item>
/// </list>
/// </summary>
/// <param name="pipelineName">The name of the pipeline</param>
/// <param name="logger">Logger for tracing</param>
/// <param name="deserializer">Step that deserializes CloudEvent data into TInput</param>
/// <param name="extractors">Steps that extract data from the input</param>
/// <param name="validator">Step that validates the input</param>
/// <param name="transformer">Step that transforms TInput to TOutput</param>
/// <param name="sender">Step that sends data to the external system</param>
/// <param name="idempotencyChecker">Step that checks for duplicate processing</param>
/// <param name="idempotencyRecorder">Step that records successful processing</param>
/// <param name="businessIncidentRouter">Finalizer that routes business incidents</param>
/// <param name="receiptSender">Optional step that sends a receipt after a successful load</param>
/// <typeparam name="TInput">The input type (deserialized from CloudEvent.Data)</typeparam>
/// <typeparam name="TOutput">The output type (sent to external system)</typeparam>
/// <typeparam name="TCtx">The context type</typeparam>
public class Loader<TInput, TOutput, TCtx>(
    string pipelineName,
    ILogger logger,
    DeserializeStep<TInput, TCtx> deserializer,
    IReadOnlyList<ExtractStep<TInput, TCtx>> extractors,
    ValidateStep<TInput, TCtx> validator,
    TransformStep<TInput, TOutput, TCtx> transformer,
    SendStep<TOutput, TCtx> sender,
    IdempotencyCheckStep<TInput, TCtx> idempotencyChecker,
    IdempotencyRecordStep<TOutput, TCtx> idempotencyRecorder,
    BusinessIncidentRouteStep<TOutput, TCtx> businessIncidentRouter,
    SendStep<TOutput, TCtx>? receiptSender = null)
    where TCtx : Context
{
    /// <summary>
    /// Executes the loader pipeline.
    /// </summary>
    /// <param name="input">The CloudEvent to process</param>
    /// <param name="context">The pipeline context</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution</param>
    /// <returns>The result of the pipeline execution and the final context</returns>
    public async Task<(StepResult<TOutput> Result, TCtx Context)> Execute(CloudEvent input, TCtx context,
        CancellationToken ct = default)
    {
        return await PipelineTracing.ExecuteWithTracing(async () =>
            {
                var pipeline = Pipeline
                    .Start(input, context, ct)
                    .AddStep(deserializer)
                    .AddSteps(extractors)
                    .AddStep(validator)
                    .AddStep(idempotencyChecker)
                    .AddStep(transformer)
                    .AddStep(sender)
                    .AddStep(idempotencyRecorder);

                if (receiptSender != null)
                    pipeline = pipeline.AddStep(receiptSender);

                return await pipeline.AddFinalizer(businessIncidentRouter);
            },
            pipelineName: pipelineName,
            logger: logger,
            configureActivity: _ => { },
            detachTrace: true
        );
    }
}
