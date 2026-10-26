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
    public record Success(T Value) : StepResult<T>;

    /// <summary>
    /// Represents a successful result whose item's business failure was handled by being delivered
    /// to the Business Incident Service. Every consumer that treats a success as a handled item —
    /// a file sweep that completes its file, a send pipeline that acknowledges its message — sees
    /// this as the success it ends the pipeline as; a host that answers a caller instead matches
    /// this subtype, to tell the routed item from one that published.
    /// </summary>
    /// <param name="Value">The value to return, as on any successful result.</param>
    /// <param name="Description">The incident's description — why the item was routed — as the
    /// failing step said it, or <c>null</c> when it did not.</param>
    public sealed record RoutedIncident(T Value, string? Description) : Success(Value);

    /// <summary>
    /// Represents a cancelled result from a <see cref="Step{TIn,TOut,TCtx}"/>.
    /// </summary>
    public sealed record Cancelled : StepResult<T>;

    /// <summary>
    /// Represents a failed result from a <see cref="Step{TIn,TOut,TCtx}"/>, where the failure domain is 'Business'.
    /// </summary>
    /// <param name="Value">A BusinessIncident to be handled by a later step</param>
    /// <param name="Retriable">Whether the item should be tried again once the incident is routed.
    /// <para>
    /// <see langword="true"/>, the default, says the failure was in the circumstances rather than in
    /// the data — a destination in a maintenance window, a dependency that is down — so the same
    /// item would succeed on a later attempt: the incident is routed <em>and</em> the item is left
    /// for redelivery. It is the default because consuming an item destroys it, while leaving it
    /// for redelivery is recoverable and still ends in the broker's dead-letter queue.
    /// </para>
    /// <para>
    /// <see langword="false"/> is a verdict on the data itself — a rejected record, an unknown
    /// reference — which no further attempt changes: routing the incident is the whole of the
    /// handling, and the item is consumed.
    /// </para>
    /// <para>
    /// It governs redelivery, so it has no effect on an item swept from a source port: a sweep has
    /// no redelivery budget to spend and no dead-letter queue to end in, so a routed incident is
    /// always the terminal handling of a source file.
    /// </para></param>
    public sealed record BusinessFailure(BusinessIncidentData Value, bool Retriable = true) : StepResult<T>;

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
