using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive;

/// <summary>
/// A pipeline used on the publish side of a message-based integration flow. It follows these steps:
/// <list type="number">
///   <item>Receive: reads content from a source (e.g., file adapter)</item>
///   <item>Enqueue: publishes the content to a message queue</item>
///   <item>Complete: handles cleanup (delete/archive source)</item>
///   <item>Route business incidents: routes any business incidents that occurred during the execution</item>
/// </list>
/// </summary>
/// <typeparam name="TCtx">The type of the context used in the pipeline.</typeparam>
/// <param name="pipelineName">The name of the pipeline. Used for tracing purposes.</param>
/// <param name="logger">An instance of <see cref="ILogger"/>.</param>
/// <param name="receiver">The step that reads content from the source.</param>
/// <param name="enqueuer">The step that publishes content to the queue.</param>
/// <param name="completer">The step that handles cleanup.</param>
/// <param name="businessIncidentRouter">The finalizer that routes business incidents.</param>
public class ReceivePipeline<TCtx>(
    string pipelineName,
    ILogger logger,
    ReceiveStep<TCtx> receiver,
    EnqueueStep<TCtx> enqueuer,
    CompleteStep<TCtx> completer,
    BusinessIncidentRouteStep<SourceItem, TCtx> businessIncidentRouter) : IReceivePipeline<TCtx> where TCtx : Context
{
    /// <summary>
    /// Executes the pipeline for a single source item.
    /// </summary>
    /// <param name="itemInfo">Information about the item to process.</param>
    /// <param name="context">The context to use in the execution.</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution.</param>
    /// <returns>A tuple of the final result of the pipeline execution and the final context.</returns>
    public virtual async Task<(StepResult<SourceItem> Result, TCtx Context)> Execute(SourceItemInfo itemInfo,
        TCtx context, CancellationToken ct = default)
    {
        return await PipelineTracing.ExecuteWithTracing(async () => await Pipeline
                .Start(itemInfo, context, ct)
                .AddStep(receiver)
                .AddStep(enqueuer)
                .AddStep(completer)
                .AddFinalizer(businessIncidentRouter),
            pipelineName: pipelineName,
            logger: logger,
            configureActivity: activity => activity?.SetTag("source_item_id", itemInfo.Id),
            detachTrace: true
        );
    }
}
