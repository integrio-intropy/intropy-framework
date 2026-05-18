using Intropy.Framework.Core.Pipeline.Abstractions.Steps;
using Intropy.Contracts.BusinessIncidentService;

namespace Intropy.Framework.Core.Pipeline.Abstractions.Results;

/// <summary>
/// Represents the result from a <see cref="BusinessStep{TIn,TOut,TCtx}"/>.
/// </summary>
/// <code>
/// var successResult = new BusinessStepResult&lt;T&gt;.Success(new T(...));
/// var failureResult = new BusinessStepResult&lt;T&gt;.Failure(new BusinessFailure(...));
/// </code>
/// <typeparam name="T">The type of the value, in case of a successful result</typeparam>
public abstract record BusinessStepResult<T>
{
    /// <summary>
    /// Represents a successful result from a <see cref="BusinessStep{TIn,TOut,TCtx}"/>.
    /// </summary>
    /// <param name="Value">The value to return</param>
    public sealed record Success(T Value) : BusinessStepResult<T>;

    /// <summary>
    /// Represents a cancelled result from a <see cref="BusinessStep{TIn,TOut,TCtx}"/>.
    /// </summary>
    public sealed record Cancelled : BusinessStepResult<T>;

    /// <summary>
    /// Represents a failed result from a <see cref="BusinessStep{TIn,TOut,TCtx}"/>, where the failure domain is 'Business'.
    /// </summary>
    /// <param name="Value">A BusinessIncident to be handled by a later step</param>
    public sealed record Failure(BusinessIncidentData Value) : BusinessStepResult<T>;

    /// <summary>
    /// Represents an aborted result from a <see cref="BusinessStep{TIn,TOut,TCtx}"/>, caused by cancellation token signalling.
    /// </summary>
    public sealed record Aborted : BusinessStepResult<T>;

    /// <summary>
    /// Converts a <see cref="BusinessStepResult{T}"/> to a <see cref="StepResult{T}"/>
    /// </summary>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when the result is of unknown type</exception>
    internal StepResult<T> ToStepResult()
    {
        return this switch
        {
            Success s => new StepResult<T>.Success(s.Value),
            Cancelled => new StepResult<T>.Cancelled(),
            Failure bf => new StepResult<T>.BusinessFailure(bf.Value),
            Aborted => new StepResult<T>.Aborted(),
            _ => throw new InvalidOperationException("Unknown business result type")
        };
    }

    /// <summary>
    /// Converts a <see cref="BusinessStepResult{T}"/> to a human-readable string.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when the result is of unknown type</exception>
    internal string GetResultType()
    {
        return this switch
        {
            Success => "success",
            Cancelled => "cancelled",
            Failure => "business_failure",
            Aborted => "aborted",
            _ => throw new InvalidOperationException("Unknown business result type")
        };
    }
}
