using Intropy.Framework.Core.Pipeline.Abstractions.Steps;
using Intropy.Contracts.BusinessIncidentService;

namespace Intropy.Framework.Core.Pipeline.Abstractions.Results;

/// <summary>
/// Represents the result from a step.
/// </summary>
/// <code>
/// var successResult = new StepResult&lt;T&gt;.Success(new T(...));
/// var failureResult = new StepResult&lt;T&gt;.BusinessFailure(new BusinessFailure(...));
/// </code>
/// <typeparam name="T">The type of value to return, in case of a successful result</typeparam>
public abstract record StepResult<T>
{
    /// <summary>
    /// Represents a successful result from a <see cref="Step{TIn,TOut,TCtx}"/>.
    /// </summary>
    /// <param name="Value">The value to return</param>
    public sealed record Success(T Value) : StepResult<T>;

    /// <summary>
    /// Represents a cancelled result from a <see cref="Step{TIn,TOut,TCtx}"/>.
    /// </summary>
    public sealed record Cancelled : StepResult<T>;

    /// <summary>
    /// Represents a failed result from a <see cref="Step{TIn,TOut,TCtx}"/>, where the failure domain is 'Business'.
    /// </summary>
    /// <param name="Value">A BusinessIncident to be handled by a later step</param>
    public sealed record BusinessFailure(BusinessIncidentData Value) : StepResult<T>;

    /// <summary>
    /// Represents a failed result from a <see cref="Step{TIn,TOut,TCtx}"/>, where the failure domain is 'Technical'.
    /// </summary>
    /// <param name="Value">A TechnicalFailure with information about the failure.</param>
    public sealed record TechnicalFailure(Pipeline.Abstractions.Failures.TechnicalFailure Value) : StepResult<T>;

    /// <summary>
    /// Represents an aborted result from a <see cref="Step{TIn,TOut,TCtx}"/>, caused by cancellation token signalling.
    /// </summary>
    public sealed record Aborted : StepResult<T>;

    /// <summary>
    /// Converts a <see cref="StepResult{T}"/> to a human-readable string.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when the result is of unknown type</exception>
    internal string GetResultType()
    {
        return this switch
        {
            Success => "success",
            Cancelled => "cancelled",
            BusinessFailure => "business_failure",
            TechnicalFailure => "technical_failure",
            Aborted => "aborted",
            _ => "unknown"
        };
    }
}
