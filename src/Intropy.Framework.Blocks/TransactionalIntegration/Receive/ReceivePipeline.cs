using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive;

/// <summary>
/// The receive side of a message-based integration flow. It follows these steps:
/// <list type="number">
///   <item>Enqueue: publishes the item's content to the integration's queue</item>
///   <item>Route business incidents (optional): routes any business incidents that occurred during the execution</item>
/// </list>
/// The file sweep that runs this pipeline reads the source item before it, and completes the
/// source (delete or archive) only after it succeeds — never before the content is on the queue.
/// </summary>
/// <typeparam name="TCtx">The type of the context used in the pipeline.</typeparam>
/// <param name="pipelineName">The name of the pipeline. Used for tracing purposes.</param>
/// <param name="logger">An instance of <see cref="ILogger"/>.</param>
/// <param name="enqueuer">The step that publishes content to the queue.</param>
/// <param name="businessIncidentRouter">Optional finalizer that routes business incidents. Skipped when null.</param>
public class ReceivePipeline<TCtx>(
    string pipelineName,
    ILogger logger,
    EnqueueStep<TCtx> enqueuer,
    BusinessIncidentRouteStep<SourceItem, TCtx>? businessIncidentRouter) : IReceivePipeline<TCtx> where TCtx : Context
{
    /// <inheritdoc />
    public virtual async Task<(StepResult<SourceItem> Result, TCtx Context)> Execute(SourceItem item,
        TCtx context, bool detachTrace = false, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(item);
        return await PipelineTracing.ExecuteWithTracing(async () => await Pipeline
                .Start(item, context, ct)
                .AddStep(enqueuer)
                .AddOptionalFinalizer(businessIncidentRouter),
            pipelineName: pipelineName,
            logger: logger,
            configureActivity: activity => activity?.SetTag("source_item_id", item.Id),
            detachTrace: detachTrace
        );
    }
}
