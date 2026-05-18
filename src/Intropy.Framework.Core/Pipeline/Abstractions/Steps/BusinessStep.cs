using System.Diagnostics;
using Intropy.Framework.Core.Common;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Contracts.BusinessIncidentService;

namespace Intropy.Framework.Core.Pipeline.Abstractions.Steps;

/// <summary>
/// An abstract step with failure domain 'Business'.
/// </summary>
/// <typeparam name="TIn">The type that enters into the step</typeparam>
/// <typeparam name="TOut">The type that exits from the step</typeparam>
/// <typeparam name="TCtx">The type of the context used in the step</typeparam>
public abstract class BusinessStep<TIn, TOut, TCtx>
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
    public abstract Task<(BusinessStepResult<TOut> Result, TCtx Context)> ExecuteAsync(TIn input, TCtx context,
        CancellationToken ct);

    /// <summary>
    /// Wrapper method for the <see cref="ExecuteAsync"/> method that adds exception handling and tracing.
    /// </summary>
    /// <param name="input">The value from the previous step, if that step succeeded.</param>
    /// <param name="context">The context passed from the previous step.</param>
    /// <param name="ct">A cancellation token that can be used to abort the step.</param>
    /// <returns></returns>
    internal async Task<(BusinessStepResult<TOut> Result, TCtx Context)> ExecuteAsyncInternal(TIn input, TCtx context,
        CancellationToken ct)
    {
        using var activity = ActivitySourceProvider.ActivitySource.StartActivity($"Step.{StepName}");
        SetInitialActivityTags(activity);

        if (ct.IsCancellationRequested)
        {
            SetFinalActivityStatus(activity, new BusinessStepResult<TOut>.Aborted());
            return (new BusinessStepResult<TOut>.Aborted(), context);
        }

        try
        {
            var (result, newContext) = await ExecuteAsync(input, context, ct);

            SetFinalActivityStatus(activity, result);

            return (result, newContext);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            SetFinalActivityStatus(activity, new BusinessStepResult<TOut>.Aborted());
            return (new BusinessStepResult<TOut>.Aborted(), context);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Ok);
            activity?.AddException(ex);
            var businessIncident = CreateBusinessIncident(ex);
            return (new BusinessStepResult<TOut>.Failure(businessIncident), context);
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
    /// <param name="result">The <see cref="BusinessStepResult{T}"/> from the step execution</param>
    private static void SetFinalActivityStatus(Activity? activity, BusinessStepResult<TOut> result)
    {
        if (activity is null) return;

        var resultType = result.GetResultType();
        activity.SetTag("step.result", resultType);
        activity.SetStatus(ActivityStatusCode.Ok);

        if (result is BusinessStepResult<TOut>.Failure bf)
        {
            activity.SetTag("step.business_failure_message", bf.Value.Description);
        }
    }

    /// <summary>
    /// Helper method for converting an uncaught exception into a <see cref="BusinessIncidentData"/>.
    /// </summary>
    /// <param name="exception">The exception that occurred.</param>
    /// <returns></returns>
    private BusinessIncidentData CreateBusinessIncident(Exception exception)
    {
        var description = $"An unhandled exception occurred in the '{StepName}' step.";
        var context = new Dictionary<string, string>
        {
            { "exceptionType", exception.GetType().FullName ?? "Exception type not available" },
            { "message", exception.Message }
        };

        var businessIncident = new BusinessIncidentData { Description = description, Context = context };
        return businessIncident;
    }
}
