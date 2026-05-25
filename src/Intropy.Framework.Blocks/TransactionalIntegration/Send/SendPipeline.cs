using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Send;

/// <summary>
/// A pipeline used on the receiving end of a message based integration flow. It follows these steps:
/// <list type="number">
///   <item>Deserialize: deserializes the string data into a POCO</item>
///   <item>Idempotency check (optional): checks so that the same data has not been processed already</item>
///   <item>Extract: extracts extra data that is required during processing. E.g. ID lookups, enrich data, etc.</item>
///   <item>Validate: validates the POCO</item>
///   <item>Transform: transforms the input POCO into an output POCO</item>
///   <item>Serialize: serializes the output POCO into string</item>
///   <item>Send: sends the string data to the destination</item>
///   <item>Record idempotency (optional): records that the data has been processed</item>
///   <item>Route business incidents (optional): routes any business incidents that occurred during the execution</item>
/// </list>
/// </summary>
/// <param name="pipelineName">The name of the pipeline. Used for tracing purposes.</param>
/// <param name="logger">An instance of <see cref="ILogger"/></param>
/// <param name="deserializer">Custom deserialize step.</param>
/// <param name="extractors">Custom extract steps.</param>
/// <param name="validator">Custom validation step.</param>
/// <param name="transformer">Custom transform step.</param>
/// <param name="serializer">Custom serialize step.</param>
/// <param name="sender">Custom send step.</param>
/// <param name="idempotencyChecker">Optional standard or custom idempotency check step. Skipped when null.</param>
/// <param name="idempotencyRecorder">Optional standard or custom idempotency record step. Skipped when null.</param>
/// <param name="businessIncidentRouter">Optional standard or custom business incident route step. Skipped when null.</param>
/// <typeparam name="TInput">The type of the POCO that enters the pipeline.</typeparam>
/// <typeparam name="TOutput">The type of the POCO that exists the pipeline.</typeparam>
/// <typeparam name="TCtx">The type of the context using in the pipeline.</typeparam>
public class SendPipeline<TInput, TOutput, TCtx>(
    string pipelineName,
    ILogger logger,
    DeserializeStep<TInput, TCtx> deserializer,
    IReadOnlyList<ExtractStep<TInput, TCtx>> extractors,
    ValidateStep<TInput, TCtx> validator,
    TransformStep<TInput, TOutput, TCtx> transformer,
    SerializeStep<TOutput, TCtx> serializer,
    SendStep<TCtx> sender,
    IdempotencyCheckStep<TInput, TCtx>? idempotencyChecker,
    IdempotencyRecordStep<string, TCtx>? idempotencyRecorder,
    BusinessIncidentRouteStep<string, TCtx>? businessIncidentRouter) : ISendPipeline<TCtx> where TCtx : Context
{
    /// <summary>
    /// Executes the pipeline
    /// </summary>
    /// <param name="input">The data to process, represented as string.</param>
    /// <param name="context">The context to use in the execution.</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution.</param>
    /// <returns>A tuple of the final result of the pipeline execution and the final context.</returns>
    public async Task<(StepResult<string> Result, TCtx Context)> Execute(ReadOnlyMemory<byte> input, TCtx context,
        CancellationToken ct = default)
    {
        return await PipelineTracing.ExecuteWithTracing(async () => await Pipeline
                .Start(input, context, ct)
                .AddStep(deserializer)
                .AddSteps(extractors)
                .AddOptionalStep(idempotencyChecker)
                .AddStep(validator)
                .AddStep(transformer)
                .AddStep(serializer)
                .AddStep(sender)
                .AddOptionalStep(idempotencyRecorder)
                .AddOptionalFinalizer(businessIncidentRouter),
            pipelineName: pipelineName,
            logger: logger,
            configureActivity: _ => { }
        );
    }
}
