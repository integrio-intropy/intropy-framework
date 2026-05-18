using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Core.Pipeline.Abstractions.Results;

/// <summary>
/// Represents the result from a <see cref="TechnicalStep{TIn,TOut,TCtx}"/>.
/// </summary>
/// <code>
/// var successResult = new TechnicalStepResult&lt;T&gt;.Success(new T(...));
/// var failureResult = new TechnicalStepResult&lt;T&gt;.Failure(new TechnicalFailure(...));
/// </code>
/// <typeparam name="T"></typeparam>
public abstract record TechnicalStepResult<T>
{
    /// <summary>
    /// Represents a successful result from a <see cref="TechnicalStep{TIn,TOut,TCtx}"/>.
    /// </summary>
    /// <param name="Value">The value to return</param>
    public sealed record Success(T Value) : TechnicalStepResult<T>;

    /// <summary>
    /// Represents a cancelled result from a <see cref="TechnicalStep{TIn,TOut,TCtx}"/>.
    /// </summary>
    public sealed record Cancelled : TechnicalStepResult<T>;

    /// <summary>
    /// Represents a failed result from a <see cref="TechnicalStep{TIn,TOut,TCtx}"/>, where the failure domain is 'Technical'.
    /// </summary>
    /// <param name="Value">A <see cref="TechnicalFailure"/> to be handled by a later step</param>
    public sealed record Failure(TechnicalFailure Value) : TechnicalStepResult<T>;

    /// <summary>
    /// Represents an aborted result from a <see cref="TechnicalStep{TIn,TOut,TCtx}"/>, caused by cancellation token signalling.
    /// </summary>
    public sealed record Aborted : TechnicalStepResult<T>;

    /// <summary>
    /// Converts a <see cref="TechnicalStepResult{T}"/> to a <see cref="StepResult{T}"/>
    /// </summary>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when the result is of unknown type</exception>
    public StepResult<T> ToStepResult()
    {
        return this switch
        {
            Success s => new StepResult<T>.Success(s.Value),
            Cancelled => new StepResult<T>.Cancelled(),
            Failure tf => new StepResult<T>.TechnicalFailure(tf.Value),
            Aborted => new StepResult<T>.Aborted(),
            _ => throw new InvalidOperationException("Unknown technical result type")
        };
    }

    /// <summary>
    /// Converts a <see cref="TechnicalStepResult{T}"/> to a human-readable string.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when the result is of unknown type</exception>
    internal string GetResultType()
    {
        return this switch
        {
            Success => "success",
            Cancelled => "cancelled",
            Failure => "technical_failure",
            Aborted => "aborted",
            _ => "unknown"
        };
    }
}