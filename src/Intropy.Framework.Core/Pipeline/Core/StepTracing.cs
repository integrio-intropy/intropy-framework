using System.Diagnostics;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Core.Pipeline.Core;

/// <summary>
/// Records a step's result on its span, the same way for every step: for a step that runs outside
/// <see cref="Abstractions.Steps.Step{TIn,TOut,TCtx}"/> but should read like one in a trace.
/// </summary>
public static class StepTracing
{
    /// <summary>
    /// Tags <paramref name="activity"/> with <paramref name="result"/>: its type, and the failure's
    /// description for a business or technical failure. Only a technical failure is an error.
    /// </summary>
    /// <param name="activity">The step's span; nothing is recorded when it is null.</param>
    /// <param name="result">The step's result.</param>
    /// <typeparam name="T">The type of the value the result carries.</typeparam>
    public static void SetResult<T>(Activity? activity, StepResult<T> result)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (activity is null) return;

        var resultType = result.GetResultType();
        activity.SetTag("step.result", resultType);
        activity.SetStatus(resultType == "technical_failure"
            ? ActivityStatusCode.Error
            : ActivityStatusCode.Ok);

        if (result is StepResult<T>.BusinessFailure bf)
        {
            activity.SetTag("step.business_failure_message", bf.Value.Description);
            activity.SetTag("step.business_failure_retriable", bf.Retriable);
        }
        else if (result is StepResult<T>.TechnicalFailure tf)
        {
            activity.SetTag("step.technical_failure_message", tf.Value.Description);
        }
    }
}
