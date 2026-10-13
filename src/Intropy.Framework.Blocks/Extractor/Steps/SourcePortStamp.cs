using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// The send step of a pipeline for one source port: stamps the port on the CloudEvent as the
/// <see cref="CloudEventExtensionKeys.SourcePort"/> extension, then sends it with the configured
/// send step. The port comes from the builder, never from the context, so a step cannot change it.
/// </summary>
internal sealed class SourcePortStamp<TCtx>(SendStep<TCtx> sender, string sourcePort) : SendStep<TCtx>
    where TCtx : Context
{
    private static readonly CloudEventAttribute Attribute =
        CloudEventAttribute.CreateExtension(CloudEventExtensionKeys.SourcePort, CloudEventAttributeType.String);

    /// <inheritdoc/>
    public override string StepName => sender.StepName;

    /// <inheritdoc/>
    public override Task<(TechnicalStepResult<CloudEvent> Result, TCtx Context)> ExecuteAsync(CloudEvent input,
        TCtx context, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(input);
        input[Attribute] = sourcePort;
        return sender.ExecuteAsync(input, context, ct);
    }
}
