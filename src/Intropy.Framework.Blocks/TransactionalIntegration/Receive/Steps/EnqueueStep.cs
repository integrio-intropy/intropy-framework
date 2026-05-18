using System.Diagnostics;
using CloudNative.CloudEvents.SystemTextJson;
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

    /// <inheritdoc />
    public sealed override async Task<(TechnicalStepResult<SourceItem> Result, TCtx Context)> ExecuteAsync(
        SourceItem input, TCtx context, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(context);

        var activity = Activity.Current;
        var cloudEvent = CloudEventHelper.Create(input, context, activity, options);
        var formatter = new JsonEventFormatter();
        var bytes = formatter.EncodeStructuredModeMessage(cloudEvent, out _);
        return await ExecuteAsync(input, bytes, context, ct);
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
