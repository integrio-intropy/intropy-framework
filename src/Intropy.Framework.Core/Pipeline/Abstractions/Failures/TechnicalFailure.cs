using System.Diagnostics.CodeAnalysis;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Core.Pipeline.Abstractions.Failures;

/// <summary>
/// Represents a technical failure that occurred during pipeline execution.
/// Technical failures indicate infrastructure or system-level issues (e.g., network errors, database unavailability, file system errors).
/// </summary>
/// <param name="Description">Short description of the failure</param>
/// <param name="ErrorMessage">Extensive error message as a structured message template</param>
/// <param name="Exception">Exception that occurred</param>
/// <param name="ErrorMessageArgs">Params for the <see cref="ErrorMessage"/></param>
[SuppressMessage("Performance", "CA1819:Properties should not return arrays",
    Justification = "ErrorMessageArgs uses params to mirror ILogger.LogError ergonomics.")]
public record TechnicalFailure(
    string Description,
    [StructuredMessageTemplate] string? ErrorMessage = null,
    Exception? Exception = null,
    params object?[]? ErrorMessageArgs)
{
    [SuppressMessage("Usage", "CA2254:Template should be a static expression",
        Justification = "ErrorMessage is a forwarded structured message template provided by the caller.")]
    internal void Log(ILogger logger)
    {
        logger.LogError(exception: Exception, message: ErrorMessage, args: ErrorMessageArgs ?? []);
    }

    internal static TechnicalFailure FromExceptionInPipeline(Exception exception, string pipelineName)
    {
        return new TechnicalFailure(
            Description: "Uncaught exception",
            Exception: exception,
            ErrorMessage: "Uncaught exception in pipeline {pipeline.name}",
            ErrorMessageArgs: pipelineName
        );
    }

    internal static TechnicalFailure FromExceptionInStep(Exception exception, string stepName)
    {
        return new TechnicalFailure(
            Description: "Uncaught exception",
            Exception: exception,
            ErrorMessage: "Uncaught exception in step {step.name}",
            ErrorMessageArgs: stepName
        );
    }
}
