using System.Diagnostics;
using Intropy.Framework.Core.Common;
using Intropy.Framework.Core.Pipeline.Abstractions.Enums;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Core.Pipeline.Abstractions.Steps;

/// <summary>
/// An abstract finalizer.
/// </summary>
/// <typeparam name="T">The type that enters into the finalizer</typeparam>
/// <typeparam name="TCtx">The type of the context used in the finalizer</typeparam>
public abstract class Finalizer<T, TCtx>
{
    /// <summary>
    /// The name of the finalizer.
    /// </summary>
    public abstract string FinalizerName { get; }

    /// <summary>
    /// Flag that tells the pipeline the condition for when the finalizer should be run.
    /// </summary>
    public abstract FinalizerTrigger Triggers { get; }

    /// <summary>
    /// The method that will be executed once the step is run.
    /// </summary>
    /// <param name="result">The result from the previous step.</param>
    /// <param name="context">The context passed from the previous step.</param>
    /// <param name="ct">A cancellation token that can be used to abort the finalizer.</param>
    /// <returns></returns>
    public abstract Task<(StepResult<T> Result, TCtx Context)> ExecuteAsync(StepResult<T> result, TCtx context, CancellationToken ct);

    /// <summary>
    /// Wrapper method for the <see cref="ExecuteAsync"/> method that adds exception handling and tracing.
    /// </summary>
    /// <param name="result">The result from the previous step.</param>
    /// <param name="context">The context passed from the previous step.</param>
    /// <param name="ct">A cancellation token that can be used to abort the finalizer.</param>
    /// <returns></returns>
    internal async Task<(StepResult<T> Result, TCtx Context)> ExecuteAsyncInternal(StepResult<T> result, TCtx context, CancellationToken ct)
    {
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity($"Finalizer.{FinalizerName}");
        SetInitialActivityTags(activity);

        if (ct.IsCancellationRequested)
        {
            SetFinalActivityStatus(activity, new StepResult<T>.Aborted());
            return (new StepResult<T>.Aborted(), context);
        }

        try
        {
            var (newResult, newContext) = await ExecuteAsync(result, context, ct);

            SetFinalActivityStatus(activity, newResult);

            return (newResult, newContext);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            SetFinalActivityStatus(activity, new StepResult<T>.Aborted());
            return (new StepResult<T>.Aborted(), context);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            var technicalFailure = TechnicalFailure.FromExceptionInStep(ex, FinalizerName);
            return (new StepResult<T>.TechnicalFailure(technicalFailure), context);
        }
    }

    /// <summary>
    /// Sets tags on the activity before the finalizer is executed.
    /// </summary>
    /// <param name="activity">The <see cref="Activity"/> to act on</param>
    private void SetInitialActivityTags(Activity? activity)
    {
        if (activity is null) return;

        activity.SetTag("finalizer.name", FinalizerName);
        activity.SetTag("finalizer.triggers", Triggers.ToString());
    }

    /// <summary>
    /// Sets properties on the activity after the finalizer has been executed.
    /// </summary>
    /// <param name="activity">The <see cref="Activity"/> to act on</param>
    /// <param name="result">The <see cref="StepResult{T}"/> from the finalizer execution</param>
    private static void SetFinalActivityStatus(Activity? activity, StepResult<T> result)
    {
        if (activity is null) return;

        var resultType = result.GetResultType();
        activity.SetTag("finalizer.output_result", resultType);
        activity.SetStatus(resultType == "technical_failure"
            ? ActivityStatusCode.Error
            : ActivityStatusCode.Ok);
    }
}