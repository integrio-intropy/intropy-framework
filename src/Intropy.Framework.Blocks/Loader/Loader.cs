using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.Loader;

/// <summary>
/// A pipeline template used for loader components. It follows these steps:
/// <list type="number">
///   <item>Archive (optional, archive mode OnReceipt): copies the received payload to the archive store</item>
///   <item>Deserialize: deserializes the message's payload bytes into a POCO</item>
///   <item>Extract: extracts data from the input</item>
///   <item>Validate: validates the POCO</item>
///   <item>Idempotency check (optional): checks so that the same data has not been processed already</item>
///   <item>Transform: transforms the input POCO into an output POCO</item>
///   <item>Send: sends the data to the external system</item>
///   <item>Record idempotency (optional): records that the data has been processed</item>
///   <item>Send receipt (optional): sends a receipt after a successful load</item>
///   <item>Archive incident (optional): makes sure an incident's payload is archived, and names the copy on the incident</item>
///   <item>Route business incidents: routes any business incidents that occurred during the execution</item>
/// </list>
/// </summary>
/// <remarks>
/// The pipeline's input is the payload, not the message it arrived in: the message's own attributes
/// are read from the context, where its caller puts them before the pipeline runs (see
/// <see cref="CloudEventContextKeys"/>). A host projects them from the delivery it received; a
/// standalone caller gets the same projection from the
/// <see cref="Execute(CloudEvent,TCtx,bool,CancellationToken)"/> overload. Idempotency, when
/// configured, reads <see cref="CloudEventContextKeys.Subject"/> (unless the builder was given its
/// own ID extractor) and <see cref="CloudEventContextKeys.Time"/> from there, so a context without
/// them fails the pipeline.
/// </remarks>
/// <param name="pipelineName">The name of the pipeline</param>
/// <param name="logger">Logger for tracing</param>
/// <param name="deserializer">Step that deserializes the payload bytes into TInput</param>
/// <param name="extractors">Steps that extract data from the input</param>
/// <param name="validator">Step that validates the input</param>
/// <param name="transformer">Step that transforms TInput to TOutput</param>
/// <param name="sender">Step that sends data to the external system</param>
/// <param name="idempotencyChecker">Optional step that checks for duplicate processing. Skipped when null.</param>
/// <param name="idempotencyRecorder">Optional step that records successful processing. Skipped when null.</param>
/// <param name="businessIncidentRouter">Finalizer that routes business incidents</param>
/// <param name="receiptSender">Optional step that sends a receipt after a successful load</param>
/// <param name="archive">Optional archive the received payload is copied to: on receipt, on a
/// business incident, or both (see <see cref="ArchiveMode"/>). Skipped when null.</param>
/// <typeparam name="TInput">The input type (deserialized from the payload)</typeparam>
/// <typeparam name="TOutput">The output type (sent to external system)</typeparam>
/// <typeparam name="TCtx">The context type</typeparam>
public class Loader<TInput, TOutput, TCtx>(
    string pipelineName,
    ILogger logger,
    DeserializeStep<TInput, TCtx> deserializer,
    IReadOnlyList<ExtractStep<TInput, TCtx>> extractors,
    ValidateStep<TInput, TCtx> validator,
    TransformStep<TInput, TOutput, TCtx> transformer,
    SendStep<TOutput, TCtx> sender,
    IdempotencyCheckStep<TInput, TCtx>? idempotencyChecker,
    IdempotencyRecordStep<TOutput, TCtx>? idempotencyRecorder,
    BusinessIncidentRouteStep<TOutput, TCtx> businessIncidentRouter,
    SendStep<TOutput, TCtx>? receiptSender = null,
    ArchivePolicy? archive = null)
    where TCtx : Context
{
    /// <summary>
    /// Executes the loader pipeline.
    /// </summary>
    /// <param name="input">The message's payload: the bytes of the CloudEvent's <c>data</c> member,
    /// as the publisher encoded them.</param>
    /// <param name="context">The pipeline context, carrying the message's attributes (see
    /// <see cref="CloudEventContextKeys"/>)</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution</param>
    /// <param name="detachTrace">When false (the default), the execution continues the ambient
    /// trace — what a host sets up per message (the consumer span); when true, it starts its own
    /// trace, linked to the current one. Hosts that prepare a message trace pass false explicitly;
    /// standalone callers rarely need to detach.</param>
    /// <returns>The result of the pipeline execution and the final context</returns>
    public async Task<(StepResult<TOutput> Result, TCtx Context)> Execute(ReadOnlyMemory<byte> input, TCtx context,
        bool detachTrace = false, CancellationToken ct = default)
    {
        return await PipelineTracing.ExecuteWithTracing(async () =>
            {
                var pipeline = Pipeline
                    .Start(input, context, ct)
                    .AddOptionalStep(ArchiveSteps.OnReceipt<TCtx>(archive, logger))
                    .AddStep(deserializer)
                    .AddSteps(extractors)
                    .AddStep(validator)
                    .AddOptionalStep(idempotencyChecker)
                    .AddStep(transformer)
                    .AddStep(sender)
                    .AddOptionalStep(idempotencyRecorder);

                if (receiptSender != null)
                    pipeline = pipeline.AddStep(receiptSender);

                return await pipeline
                    .AddOptionalFinalizer(ArchiveSteps.OnIncident<TOutput, TCtx>(archive, input))
                    .AddFinalizer(businessIncidentRouter);
            },
            pipelineName: pipelineName,
            logger: logger,
            configureActivity: _ => { },
            detachTrace: detachTrace
        );
    }

    /// <summary>
    /// Executes the loader pipeline for <paramref name="input"/>: projects its attributes onto
    /// <paramref name="context"/> and runs its payload through the pipeline.
    /// </summary>
    /// <remarks>
    /// A convenience for callers holding a whole event — a standalone caller, or a test. A host does
    /// the two halves itself, because the payload of a message it consumed does not always travel
    /// inside it. Keep that in mind when using this to stand in for a subscriber: it reproduces how
    /// a payload is encoded, not how one is delivered.
    /// </remarks>
    /// <param name="input">The event to process.</param>
    /// <param name="context">The pipeline context. Its metadata receives the event's attributes,
    /// overwriting any already there.</param>
    /// <param name="detachTrace">As on <see cref="Execute(ReadOnlyMemory{byte},TCtx,bool,CancellationToken)"/>.</param>
    /// <param name="ct">A cancellation token that can be used to abort the pipeline execution</param>
    /// <returns>The result of the pipeline execution and the final context</returns>
    /// <exception cref="ArgumentNullException"><paramref name="input"/> or <paramref name="context"/>
    /// is null.</exception>
    public Task<(StepResult<TOutput> Result, TCtx Context)> Execute(CloudEvent input, TCtx context,
        bool detachTrace = false, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(context);
        Project(input, context);
        return Execute(Payload(input), context, detachTrace, ct);
    }

    /// <summary>Stamps <paramref name="input"/>'s attributes onto <paramref name="context"/>, the
    /// way a host does before it runs the pipeline. An attribute the event does not carry is left
    /// off, so a missing one surfaces where it is required rather than as an empty string. Extension
    /// attributes go on under <see cref="CloudEventContextKeys.Extension"/>, in their string form.</summary>
    internal static void Project(CloudEvent input, TCtx context)
    {
        foreach (var (attribute, value) in input.GetPopulatedAttributes())
            if (attribute.IsExtension)
                Set(CloudEventContextKeys.Extension(attribute.Name), attribute.Format(value));

        Set(CloudEventContextKeys.Id, input.Id);
        Set(CloudEventContextKeys.Subject, input.Subject);
        Set(CloudEventContextKeys.Time, input.Time?.ToString("O"));
        Set(CloudEventContextKeys.Source, input.Source?.ToString());
        Set(CloudEventContextKeys.Type, input.Type);
        Set(CloudEventContextKeys.DataContentType, input.DataContentType);
        return;

        void Set(string key, string? value)
        {
            if (!string.IsNullOrEmpty(value))
                context.Metadata[key] = value;
        }
    }

    /// <summary>The bytes of <paramref name="input"/>'s <c>data</c> member, as a subscriber receives
    /// them.</summary>
    /// <remarks>
    /// Faithful to the wire on purpose, including the case that trips people up: a payload set to
    /// pre-serialized JSON <em>text</em> is a JSON string, and a string is encoded quoted and
    /// escaped. Set <see cref="CloudEvent.Data"/> to the object itself to put a JSON object on the
    /// wire. Bytes are passed through, as the binary payload they are.
    /// </remarks>
    private static ReadOnlyMemory<byte> Payload(CloudEvent input) => input.Data switch
    {
        null => ReadOnlyMemory<byte>.Empty,
        byte[] bytes => bytes,
        ReadOnlyMemory<byte> bytes => bytes,
        var data => JsonSerializer.SerializeToUtf8Bytes(data, CloudEventFormat.SerializerOptions)
    };
}
