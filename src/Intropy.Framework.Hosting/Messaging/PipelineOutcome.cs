using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>A message's outcome, and why it failed when it did.</summary>
/// <param name="Outcome">How the message ended.</param>
/// <param name="ErrorType">The failure's type (<c>error.type</c>), set for <see cref="MessageOutcome.Failed"/>
/// and <see cref="MessageOutcome.Unrouted"/>.</param>
/// <param name="Description">A description of the failure.</param>
internal readonly record struct PipelineOutcome(MessageOutcome Outcome, string? ErrorType = null, string? Description = null)
{
    internal static PipelineOutcome Failed(string errorType, string description) =>
        new(MessageOutcome.Failed, errorType, description);

    /// <summary>Maps a pipeline's result to the message's outcome. An aborted pipeline is an
    /// interruption when <paramref name="interrupt"/> caused it, and a failure otherwise (for
    /// example the processing timeout).</summary>
    internal static PipelineOutcome From<T>(StepResult<T> result, CancellationToken interrupt) => result switch
    {
        StepResult<T>.Success => new PipelineOutcome(MessageOutcome.Processed),
        StepResult<T>.Cancelled => new PipelineOutcome(MessageOutcome.Skipped),
        StepResult<T>.Aborted when interrupt.IsCancellationRequested => new PipelineOutcome(MessageOutcome.Interrupted),
        StepResult<T>.Aborted => Failed("aborted", "Processing was aborted without the host stopping"),
        StepResult<T>.TechnicalFailure failure => Failed("technical_failure", failure.Value.Description),
        StepResult<T>.BusinessFailure failure => Failed("business_failure", failure.Value.Description),
        _ => Failed("unknown", $"The pipeline ended in an unexpected state ({result.GetType().Name})")
    };
}
