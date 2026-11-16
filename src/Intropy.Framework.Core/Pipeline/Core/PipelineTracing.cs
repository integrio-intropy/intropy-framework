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
            : StartPipelineActivity(pipelineName);

        configureActivity?.Invoke(activity);

        try
        {
            var (result, context, _) = await pipeline();
            Complete(activity, result, logger);
            return (result, context);
        }
        catch (Exception ex)
        {
            Fail(activity, ex, pipelineName, logger);
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

    /// <summary>
    /// Starts a pipeline's span without running anything under it, for a pipeline whose run the
    /// caller drives itself — one that pauses between steps, say — and ends with
    /// <see cref="Complete{T}"/> or <see cref="Fail"/>. The caller disposes it.
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="parent">The span to start it under; <see langword="default"/> starts it under
    /// the current activity, as <see cref="ExecuteWithTracing{T,TCtx}"/> does.</param>
    /// <returns>The span, or null when nothing listens.</returns>
    public static Activity? StartPipelineActivity(string pipelineName, ActivityContext parent = default)
    {
        var activity = ActivitySourceProvider.ActivitySource.StartActivity($"Pipeline.{pipelineName}",
            ActivityKind.Internal, parent);
        activity?.SetTag("pipeline.name", pipelineName);
        return activity;
    }

    /// <summary>
    /// Records a pipeline's result on its span: a technical failure is logged and is an error;
    /// anything else, an aborted run included, is not.
    /// </summary>
    /// <param name="activity">The pipeline's span; nothing is recorded on it when it is null.</param>
    /// <param name="result">The pipeline's result.</param>
    /// <param name="logger">The logger a technical failure is logged to.</param>
    /// <typeparam name="T">The type of the value the result carries.</typeparam>
    public static void Complete<T>(Activity? activity, StepResult<T> result, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(result);
        SetActivityStatus(result, activity, logger);
    }

    /// <summary>
    /// Records an exception that escaped a pipeline on its span, and logs it as a technical failure.
    /// </summary>
    /// <param name="activity">The pipeline's span; nothing is recorded on it when it is null.</param>
    /// <param name="exception">The exception.</param>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="logger">The logger it is logged to.</param>
    public static void Fail(Activity? activity, Exception exception, string pipelineName, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(exception);
        HandleException(exception, pipelineName, activity, logger);
    }

    private static Activity? CreateDetachedActivity(string pipelineName, Activity? previousActivity)
    {
        var links = previousActivity?.Context is not null
            ? new List<ActivityLink> { new(previousActivity.Context) }
            : null;

        Activity.Current = null;

        var activity = ActivitySourceProvider.ActivitySource.StartActivity(
            name: $"Pipeline.{pipelineName}",
            kind: ActivityKind.Internal,
            links: links);
        activity?.SetTag("pipeline.name", pipelineName);
        return activity;
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
