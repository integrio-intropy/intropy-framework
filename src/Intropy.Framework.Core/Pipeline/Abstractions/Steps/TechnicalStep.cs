using System.Diagnostics;
using Intropy.Framework.Core.Common;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Core.Pipeline.Abstractions.Steps;

/// <summary>
/// An abstract step with failure domain 'Technical'.
/// </summary>
/// <typeparam name="TIn">The type that enters into the step</typeparam>
/// <typeparam name="TOut">The type that exits from the step</typeparam>
/// <typeparam name="TCtx">The type of the context used in the step</typeparam>
public abstract class TechnicalStep<TIn, TOut, TCtx>
{
    /// <summary>
    /// The name of the step.
    /// </summary>
    public abstract string StepName { get; }

    /// <summary>
    /// The method that will be executed once the step is run.
    /// </summary>
    /// <param name="input">The value from the previous step, if that step succeeded.</param>
    /// <param name="context">The context passed from the previous step.</param>
    /// <param name="ct">A cancellation token that can be used to abort the step.</param>
    /// <returns></returns>
    public abstract Task<(TechnicalStepResult<TOut> Result, TCtx Context)> ExecuteAsync(TIn input, TCtx context, CancellationToken ct);

    /// <summary>
    /// Wrapper method for the <see cref="ExecuteAsync"/> method that adds exception handling and tracing.
    /// </summary>
    /// <param name="input">The value from the previous step, if that step succeeded.</param>
    /// <param name="context">The context passed from the previous step.</param>
    /// <param name="ct">A cancellation token that can be used to abort the step.</param>
    /// <returns></returns>
    internal async Task<(TechnicalStepResult<TOut> Result, TCtx Context)> ExecuteAsyncInternal(TIn input, TCtx context, CancellationToken ct)
    {
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity($"Step.{StepName}");
        SetInitialActivityTags(activity);

        if (ct.IsCancellationRequested)
        {
            SetFinalActivityStatus(activity, new TechnicalStepResult<TOut>.Aborted());
            return (new TechnicalStepResult<TOut>.Aborted(), context);
        }

        try
        {
            var (result, newContext) = await ExecuteAsync(input, context, ct);

            SetFinalActivityStatus(activity, result);

            return (result, newContext);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            SetFinalActivityStatus(activity, new TechnicalStepResult<TOut>.Aborted());
            return (new TechnicalStepResult<TOut>.Aborted(), context);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            var technicalFailure = TechnicalFailure.FromExceptionInStep(ex, StepName);
            return (new TechnicalStepResult<TOut>.Failure(technicalFailure), context);
        }
    }

    /// <summary>
    /// Sets tags on the activity before the step is executed.
    /// </summary>
    /// <param name="activity">The <see cref="Activity"/> to act on</param>
    private void SetInitialActivityTags(Activity? activity)
    {
        activity?.SetTag("step.name", StepName);
    }

    /// <summary>
    /// Sets properties on the activity after the step has been executed.
    /// </summary>
    /// <param name="activity">The <see cref="Activity"/> to act on</param>
    /// <param name="result">The <see cref="TechnicalStepResult{T}"/> from the step execution</param>
    private static void SetFinalActivityStatus(Activity? activity, TechnicalStepResult<TOut> result)
    {
        if (activity is null) return;

        var resultType = result.GetResultType();
        activity.SetTag("step.result", resultType);
        activity.SetStatus(resultType == "technical_failure"
            ? ActivityStatusCode.Error
            : ActivityStatusCode.Ok);

        if (result is TechnicalStepResult<TOut>.Failure tf)
        {
            activity.SetTag("step.technical_failure_message", tf.Value.Description);
        }
    }
}