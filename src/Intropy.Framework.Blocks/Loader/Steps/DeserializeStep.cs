using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Loader.Steps;

/// <summary>
/// A step that takes a CloudEvent and deserializes its data into TInput.
/// Also extracts CloudEvent metadata (Id, Subject, Time, Source, Type) into the context.
/// </summary>
/// <typeparam name="TInput">The input type to the pipeline (deserialized from CloudEvent.Data)</typeparam>
/// <typeparam name="TCtx">The type of the context.</typeparam>
public abstract class DeserializeStep<TInput, TCtx> : BusinessStep<CloudEvent, TInput, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Deserialize";

    /// <summary>
    /// Override this method to provide custom deserialization logic.
    /// The CloudEvent metadata will be extracted to context automatically before this is called.
    /// </summary>
    /// <param name="cloudEvent">The CloudEvent to deserialize.</param>
    /// <param name="context">The context with CloudEvent metadata already extracted.</param>
    /// <returns>The deserialized input object and updated context.</returns>
    protected abstract Task<(BusinessStepResult<TInput> Result, TCtx Context)> DeserializeAsync(
        CloudEvent cloudEvent,
        TCtx context);

    /// <inheritdoc/>
    public sealed override async Task<(BusinessStepResult<TInput> Result, TCtx Context)> ExecuteAsync(CloudEvent input,
        TCtx context, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(context);

        // Extract CloudEvent metadata to context
        ExtractCloudEventMetadata(input, context);

        // Delegate to the user-implemented deserialization
        return await DeserializeAsync(input, context);
    }

    private static void ExtractCloudEventMetadata(CloudEvent cloudEvent, TCtx context)
    {
        if (!string.IsNullOrEmpty(cloudEvent.Id))
        {
            context.Metadata[CloudEventContextKeys.Id] = cloudEvent.Id;
        }

        if (!string.IsNullOrEmpty(cloudEvent.Subject))
        {
            context.Metadata[CloudEventContextKeys.Subject] = cloudEvent.Subject;
        }

        if (cloudEvent.Time.HasValue)
        {
            context.Metadata[CloudEventContextKeys.Time] = cloudEvent.Time.Value.ToString("O");
        }

        if (cloudEvent.Source != null)
        {
            context.Metadata[CloudEventContextKeys.Source] = cloudEvent.Source.ToString();
        }

        if (!string.IsNullOrEmpty(cloudEvent.Type))
        {
            context.Metadata[CloudEventContextKeys.Type] = cloudEvent.Type;
        }
    }
}
