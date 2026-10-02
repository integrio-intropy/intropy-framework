using System.Diagnostics;
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
        using var activity = MessagingTelemetry.StartSendActivity(DestinationName, MessagingSystem);
        try
        {
            var cloudEvent = CloudEventHelper.Create(input, context, activity ?? Activity.Current, options);
            activity?.SetTag("messaging.message.id", cloudEvent.Id);
            var bytes = CloudEventFormat.Formatter.EncodeStructuredModeMessage(cloudEvent, out _);

            var result = await ExecuteAsync(input, bytes, context, ct);
            if (result.Result is TechnicalStepResult<SourceItem>.Failure failure)
                MessagingTelemetry.Fail(activity, "technical_failure", failure.Value.Description);
            return result;
        }
        catch (Exception e) when (e is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            MessagingTelemetry.Fail(activity, e);
            throw;
        }
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
