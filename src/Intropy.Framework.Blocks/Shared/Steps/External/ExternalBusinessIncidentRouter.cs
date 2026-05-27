using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Shared.Steps.External;

/// <summary>
/// An implementation of <see cref="BusinessIncidentRouteStep{T,TCtx}"/> that uses
/// an <see cref="IBusinessIncidentServiceClient"/> to route business incidents.
/// </summary>
/// <param name="client">An instance of <see cref="IBusinessIncidentServiceClient"/></param>
/// <param name="frameworkOptions">Common config for the framework</param>
/// <param name="messageIdExtractor">A func that tells the step how to extract the messageId from the context</param>
/// <param name="subjectExtractor">A func that tells the step how to extract the subject from the context</param>
/// <param name="defaultValueFactory">A func that tells the step how to create a default value for T,
/// in case the step succeeds.</param>
/// <typeparam name="T">The type of the return value from the previous step</typeparam>
/// <typeparam name="TCtx">The type of the context</typeparam>
public class ExternalBusinessIncidentRouter<T, TCtx>(
    IBusinessIncidentServiceClient client,
    FrameworkOptions frameworkOptions,
    Func<TCtx, string> messageIdExtractor,
    Func<TCtx, string> subjectExtractor,
    Func<T> defaultValueFactory)
    : BusinessIncidentRouteStep<T, TCtx> where TCtx : Context
{
    /// <inheritdoc />
    public override async Task<(StepResult<T> Result, TCtx Context)> ExecuteAsync(StepResult<T> result, TCtx context,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(context);

        return result switch
        {
            StepResult<T>.Success s when context.IsRetry => await ResolveBusinessIncident(s, context),
            StepResult<T>.Success s => (s, context),
            StepResult<T>.BusinessFailure businessFailure => await TriggerBusinessIncident(businessFailure, context),
            _ => throw new UnreachableException(
                "Unreachable") // this.Triggers is configured to only run this step on Success | BusinessFailure
        };
    }

    private async Task<(StepResult<T> Result, TCtx context)> TriggerBusinessIncident(
        StepResult<T>.BusinessFailure businessFailure, TCtx context)
    {
        var source = FormatSource(frameworkOptions);
        var messageId = messageIdExtractor(context);
        var subject = subjectExtractor(context);
        string? batchId = null; // For future use

        try
        {
            await client.Trigger(source, subject, messageId, businessFailure.Value, batchId);
        }
        catch (BusinessIncidentServiceException e)
        {
            Activity.Current?.AddException(e);
            var tf = new TechnicalFailure("Failed to send business incident to the business incident service",
                ErrorMessage: "Error when trying to send business incident with {source} {messageId}",
                Exception: e,
                ErrorMessageArgs: [source, messageId]);
            return (new StepResult<T>.TechnicalFailure(tf), context);
        }

        return (new StepResult<T>.Success(defaultValueFactory()), context);
    }

    private async Task<(StepResult<T> Result, TCtx context)> ResolveBusinessIncident(StepResult<T> result, TCtx context)
    {
        var source = FormatSource(frameworkOptions);
        var messageId = messageIdExtractor(context);
        var subject = subjectExtractor(context);
        string? batchId = null; // For future use

        try
        {
            await client.Resolve(source, subject, messageId, batchId);
        }
        catch (BusinessIncidentServiceException e)
        {
            Activity.Current?.AddException(e);
            var tf = new TechnicalFailure(
                "Failed to send business incident resolution to the business incident service",
                ErrorMessage: "Error when trying to send business incident resolution with {id}",
                Exception: e,
                ErrorMessageArgs: [messageId]);
            return (new StepResult<T>.TechnicalFailure(tf), context);
        }

        return (result, context);
    }

    [SuppressMessage("Globalization", "CA1308:Normalize strings to uppercase",
        Justification = "URN namespace identifiers are lowercase per RFC 8141.")]
    private static Uri FormatSource(FrameworkOptions frameworkOptions)
    {
        var org = frameworkOptions.ServiceNamespace.ToLowerInvariant();
        var component = frameworkOptions.ComponentName.ToLowerInvariant();

        return new Uri($"urn:{org}:{component}");
    }
}
