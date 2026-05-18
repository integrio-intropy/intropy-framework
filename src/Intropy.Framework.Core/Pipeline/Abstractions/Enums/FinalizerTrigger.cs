namespace Intropy.Framework.Core.Pipeline.Abstractions.Enums;

/// <summary>
/// Flag used when deciding which result types a finalizer should be triggered for.
/// </summary>
[Flags]
public enum FinalizerTrigger
{
    /// <summary>
    /// Indicates that a finalized should run if the previous step was successful.
    /// </summary>
    OnSuccess = 1,
    /// <summary>
    /// Indicates that a finalized should run if the previous step was cancelled.
    /// </summary>
    OnCancelled = 2,
    /// <summary>
    /// Indicates that a finalized should run if the previous step was business failure.
    /// </summary>
    OnBusinessFailure = 4,
    /// <summary>
    /// Indicates that a finalized should run if the previous step was technical failure.
    /// </summary>
    OnTechnicalFailure = 8,
    /// <summary>
    /// Indicates that a finalizer should run if the pipeline was aborted by a cancellation token.
    /// </summary>
    OnAborted = 16
}