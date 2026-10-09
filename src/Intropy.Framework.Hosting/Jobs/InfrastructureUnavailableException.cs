namespace Intropy.Framework.Hosting.Jobs;

/// <summary>
/// The infrastructure a job depends on did not become usable, so the job could not proceed. Thrown from
/// <see cref="IJob.ExecuteAsync"/>, it makes the <see cref="JobRunner"/> exit with
/// <see cref="JobExitCodes.InfrastructureFailure"/> instead of <see cref="JobExitCodes.JobFailure"/>.
/// </summary>
public sealed class InfrastructureUnavailableException : Exception
{
    /// <summary>Creates the exception.</summary>
    public InfrastructureUnavailableException()
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">What was unavailable, and for how long the job waited.</param>
    public InfrastructureUnavailableException(string message) : base(message)
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">What was unavailable, and for how long the job waited.</param>
    /// <param name="innerException">The failure that made it unavailable.</param>
    public InfrastructureUnavailableException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
