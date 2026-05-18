using System.Diagnostics;
using Intropy.Framework.Core.Common;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Core.Pipeline.Core;

/// <summary>
/// Provides distributed tracing capabilities for pipeline execution.
/// </summary>
public static class PipelineTracing
{
    /// <summary>
    /// Executes a pipeline with distributed tracing support.
    /// </summary>
    /// <param name="pipeline">The pipeline to execute</param>
    /// <param name="pipelineName">The name of the pipeline. Used for tracing purposes.</param>
    /// <param name="logger">The logger to use.</param>
    /// <param name="configureActivity">An action for setting additional properties for the activity.</param>
    /// <param name="detachTrace">When true, creates a new root span linked to the parent. When false, continues the current trace as a child span.</param>
    /// <typeparam name="T">The type that is returned from the pipeline.</typeparam>
    /// <typeparam name="TCtx">The context that is returned from the pipeline.</typeparam>
    /// <returns></returns>
    public static async Task<(StepResult<T> Result, TCtx Context)> ExecuteWithTracing<T, TCtx>(
        Func<Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)>> pipeline,
        string pipelineName,
        ILogger logger,
        Action<Activity?>? configureActivity = null,
        bool detachTrace = false)
    {
        ArgumentNullException.ThrowIfNull(pipeline);

        var previousActivity = Activity.Current;

        using var activity = detachTrace
            ? CreateDetachedActivity(pipelineName, previousActivity)
            : CreateChildActivity(pipelineName);

        activity?.SetTag("pipeline.name", pipelineName);
        configureActivity?.Invoke(activity);

        try
        {
            var (result, context, _) = await pipeline();
            SetActivityStatus(result, activity, logger);
            return (result, context);
        }
        catch (Exception ex)
        {
            HandleException(ex, pipelineName, activity, logger);
            throw;
        }
        finally
        {
            if (detachTrace)
            {
                Activity.Current = previousActivity;
            }
        }
    }

    private static Activity? CreateDetachedActivity(string pipelineName, Activity? previousActivity)
    {
        var links = previousActivity?.Context is not null
            ? new List<ActivityLink> { new(previousActivity.Context) }
            : null;

        Activity.Current = null;

        return ActivitySourceProvider.ActivitySource.StartActivity(
            name: $"Pipeline.{pipelineName}",
            kind: ActivityKind.Internal,
            links: links);
    }

    private static Activity? CreateChildActivity(string pipelineName)
    {
        return ActivitySourceProvider.ActivitySource.StartActivity(
            name: $"Pipeline.{pipelineName}",
            kind: ActivityKind.Internal);
    }

    private static void SetActivityStatus<T>(StepResult<T> result, Activity? activity, ILogger logger)
    {
        if (result is StepResult<T>.TechnicalFailure technicalFailure)
        {
            technicalFailure.Value.Log(logger);
            activity?.SetStatus(ActivityStatusCode.Error, technicalFailure.Value.Description);
        }
        else if (result is StepResult<T>.Aborted)
        {
            activity?.SetStatus(ActivityStatusCode.Ok, "Pipeline aborted by cancellation");
        }
        else
        {
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
    }

    private static void HandleException(Exception ex, string pipelineName, Activity? activity, ILogger logger)
    {
        var technicalFailure = TechnicalFailure.FromExceptionInPipeline(ex, pipelineName);
        var result = new StepResult<object>.TechnicalFailure(technicalFailure);
        result.Value.Log(logger);
        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
    }
}
