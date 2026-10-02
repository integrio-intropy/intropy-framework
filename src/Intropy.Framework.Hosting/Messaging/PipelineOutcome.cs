using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>A message's outcome, and why it failed when it did.</summary>
/// <param name="Outcome">How the message ended.</param>
/// <param name="ErrorType">The failure's type (<c>error.type</c>), set for <see cref="MessageOutcome.Failed"/>
/// and <see cref="MessageOutcome.Unrouted"/>.</param>
/// <param name="Description">A description of the failure.</param>
/// <param name="Exception">The exception that failed the message, if one did; recorded on its span.</param>
internal readonly record struct PipelineOutcome(
    MessageOutcome Outcome,
    string? ErrorType = null,
    string? Description = null,
    Exception? Exception = null)
{
    /// <summary>The <c>error.type</c> of a message whose processing was cancelled or aborted without
    /// the host stopping. The <see cref="MessageConsumer"/> refines it to <see cref="Timeout"/> or
    /// <see cref="Cancelled"/> when it knows which caused it.</summary>
    internal const string Aborted = "aborted";

    /// <summary>The <c>error.type</c> of a message that exceeded its processing time limit.</summary>
    internal const string Timeout = "timeout";

    /// <summary>The <c>error.type</c> of a message whose delivery the sidecar abandoned.</summary>
    internal const string Cancelled = "cancelled";

    /// <summary>The <c>error.type</c> of a message no route handles.</summary>
    internal const string Unrouted = "unrouted";

    internal static PipelineOutcome Failed(string errorType, string description, Exception? exception = null) =>
        new(MessageOutcome.Failed, errorType, description, exception);

    /// <summary>Maps an exception that escaped a pipeline: an interruption when
    /// <paramref name="interrupt"/> caused it, an abort when <paramref name="cancellationToken"/>
    /// did, and a failure otherwise.</summary>
    internal static PipelineOutcome FromException(Exception exception, CancellationToken interrupt,
        CancellationToken cancellationToken) => exception switch
    {
        OperationCanceledException when interrupt.IsCancellationRequested => new PipelineOutcome(MessageOutcome.Interrupted),
        OperationCanceledException when cancellationToken.IsCancellationRequested =>
            Failed(Aborted, "Processing was cancelled without the host stopping"),
        _ => Failed(exception.GetType().FullName!, exception.Message, exception)
    };

    /// <summary>Maps a pipeline's result to the message's outcome. An aborted pipeline is an
    /// interruption when <paramref name="interrupt"/> caused it, and a failure otherwise (for
    /// example the processing timeout).</summary>
    internal static PipelineOutcome From<T>(StepResult<T> result, CancellationToken interrupt) => result switch
    {
        StepResult<T>.Success => new PipelineOutcome(MessageOutcome.Processed),
        StepResult<T>.Cancelled => new PipelineOutcome(MessageOutcome.Skipped),
        StepResult<T>.Aborted when interrupt.IsCancellationRequested => new PipelineOutcome(MessageOutcome.Interrupted),
        StepResult<T>.Aborted => Failed(Aborted, "Processing was aborted without the host stopping"),
        StepResult<T>.TechnicalFailure failure => Failed("technical_failure", failure.Value.Description),
        StepResult<T>.BusinessFailure failure => Failed("business_failure", failure.Value.Description),
        _ => Failed("unknown", $"The pipeline ended in an unexpected state ({result.GetType().Name})")
    };

    /// <summary>Names the cause of an <see cref="Aborted"/> failure: the processing time limit when
    /// <paramref name="timeout"/> fired, the sidecar abandoning the delivery when
    /// <paramref name="call"/> did.</summary>
    internal PipelineOutcome WithCancellationCause(TimeSpan limit, CancellationToken timeout, CancellationToken call)
    {
        if (Outcome != MessageOutcome.Failed || ErrorType != Aborted)
            return this;
        if (timeout.IsCancellationRequested)
            return Failed(Timeout, $"Processing exceeded its time limit of {limit}");
        if (call.IsCancellationRequested)
            return Failed(Cancelled, "The sidecar abandoned the delivery");
        return this;
    }
}
