using System.Diagnostics;
using CloudNative.CloudEvents.SystemTextJson;
using Intropy.Framework.Blocks.Common;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Helpers;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;

/// <summary>
/// Publishes item to the message queue as cloud event
/// </summary>
/// <typeparam name="TCtx">The type of the context used in the step.</typeparam>
public abstract class EnqueueStep<TCtx>(FrameworkOptions options) : TechnicalStep<SourceItem, SourceItem, TCtx>
    where TCtx : Context
{
    /// <inheritdoc />
    public override string StepName => "Enqueue";

    /// <summary>The destination the item is published to, such as the topic: names the step's
    /// send span (<c>send {destination}</c>). <see langword="null"/> when unknown.</summary>
    protected virtual string? DestinationName => null;

    /// <summary>The messaging system the item is published through (<c>messaging.system</c> on
    /// the send span). <see langword="null"/> when unknown.</summary>
    protected virtual string? MessagingSystem => null;

    /// <inheritdoc />
    public sealed override async Task<(TechnicalStepResult<SourceItem> Result, TCtx Context)> ExecuteAsync(
        SourceItem input, TCtx context, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(context);

        // The send span is the one the message carries, so the consumer's span continues it,
        // following the OpenTelemetry messaging conventions.
        using var activity = StartSendActivity();
        var cloudEvent = CloudEventHelper.Create(input, context, activity ?? Activity.Current, options);
        activity?.SetTag("messaging.message.id", cloudEvent.Id);
        var formatter = new JsonEventFormatter();
        var bytes = formatter.EncodeStructuredModeMessage(cloudEvent, out _);

        try
        {
            var result = await ExecuteAsync(input, bytes, context, ct);
            if (result.Result is TechnicalStepResult<SourceItem>.Failure failure)
                Fail(activity, "technical_failure", failure.Value.Description);
            return result;
        }
        catch (Exception e) when (e is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            activity?.AddException(e);
            Fail(activity, e.GetType().FullName!, e.Message);
            throw;
        }
    }

    private Activity? StartSendActivity()
    {
        var activity = ActivitySourceProvider.ActivitySource.StartActivity(
            DestinationName is { } destination ? $"send {destination}" : "send", ActivityKind.Producer);
        if (activity is null)
            return null;

        activity.SetTag("messaging.operation.type", "send");
        activity.SetTag("messaging.operation.name", "send");
        if (MessagingSystem is { } system)
            activity.SetTag("messaging.system", system);
        if (DestinationName is { } name)
            activity.SetTag("messaging.destination.name", name);
        return activity;
    }

    private static void Fail(Activity? activity, string errorType, string description)
    {
        activity?.SetTag("error.type", errorType);
        activity?.SetStatus(ActivityStatusCode.Error, description);
    }

    /// <summary>
    /// The method that will be executed once the step is run
    /// </summary>
    /// <param name="input">The value from the previous step, if that step succeeded</param>
    /// <param name="cloudEvent">A cloud event that has data set from the previous steps value if that step succeeded.
    /// The event is enriched with metadata from context.</param>
    /// <param name="context">The context passed from the previous step</param>
    /// <param name="ct">A cancellation token that can be used to abort the step</param>
    /// <returns></returns>
    public abstract Task<(TechnicalStepResult<SourceItem> Result, TCtx Context)> ExecuteAsync(SourceItem input,
        ReadOnlyMemory<byte> cloudEvent, TCtx context, CancellationToken ct);
}
