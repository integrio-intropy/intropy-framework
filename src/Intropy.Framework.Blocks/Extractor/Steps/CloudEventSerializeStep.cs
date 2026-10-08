using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// A reusable <see cref="SerializeStep{T,TCtx}"/> that constructs the
/// <see cref="CloudEvent"/> envelope from component-supplied extractor functions.
/// This step owns <see cref="CloudEvent.Id"/>, <see cref="CloudEvent.Subject"/>,
/// <see cref="CloudEvent.Time"/>, <see cref="CloudEvent.DataContentType"/> and
/// <see cref="CloudEvent.Data"/>, and — when a <paramref name="type"/> extractor is
/// supplied — <see cref="CloudEvent.Type"/>. The send step (e.g.
/// <see cref="DaprTopicPublisher{TCtx}"/>) owns <see cref="CloudEvent.Source"/>, supplies the
/// fallback <see cref="CloudEvent.Type"/> when this step did not set one, and performs the
/// JSON encoding at publish time.
/// </summary>
/// <typeparam name="TOutput">The output type of the pipeline.</typeparam>
/// <typeparam name="TCtx">The type of the context used in the step.</typeparam>
/// <remarks>
/// Validation of the mandatory fields is performed at serialize time: a null or whitespace
/// <paramref name="subject"/> and a <see langword="default"/> <see cref="DateTimeOffset"/> from
/// <paramref name="time"/> both surface as a technical failure naming the missing field.
/// Components whose source can legitimately produce default dates must map them before
/// extraction.
/// </remarks>
/// <param name="subject">Extracts the subject identifying the entity the event is about.</param>
/// <param name="time">Extracts the time the event occurred in the source system.</param>
/// <param name="dataContentType">The content type of the data payload. Defaults to <c>application/json</c>.</param>
/// <param name="type">Optional. Extracts the CloudEvent type from the output and context, e.g.
/// <c>(o, _) =&gt; o.Status == 0 ? "io.intropy.orders.new" : "io.intropy.orders.cancelled"</c>.
/// When supplied, the resolved type wins over the send step's configured fallback. When left
/// null, the type is left for the send step to set from its configuration.</param>
/// <exception cref="ArgumentNullException">Thrown if <paramref name="subject"/> or <paramref name="time"/> is null.</exception>
/// <exception cref="ArgumentException">Thrown if <paramref name="dataContentType"/> is null or whitespace.</exception>
public class CloudEventSerializeStep<TOutput, TCtx>(
    Func<TOutput, string> subject,
    Func<TOutput, DateTimeOffset> time,
    string dataContentType = "application/json",
    Func<TOutput, TCtx, string>? type = null) : SerializeStep<TOutput, TCtx> where TCtx : Context
{
    private readonly Func<TOutput, string> _subject =
        subject ?? throw new ArgumentNullException(nameof(subject));

    private readonly Func<TOutput, DateTimeOffset> _time =
        time ?? throw new ArgumentNullException(nameof(time));

    private readonly string _dataContentType =
        string.IsNullOrWhiteSpace(dataContentType)
            ? throw new ArgumentException("Data content type cannot be null or whitespace", nameof(dataContentType))
            : dataContentType;

    private readonly Func<TOutput, TCtx, string>? _type = type;

    /// <inheritdoc/>
    public override Task<(TechnicalStepResult<CloudEvent> Result, TCtx Context)> ExecuteAsync(
        TOutput input, TCtx context, CancellationToken ct)
    {
        var subjectValue = _subject(input);
        if (string.IsNullOrWhiteSpace(subjectValue))
        {
            var tf = new TechnicalFailure(
                "CloudEvent.Subject must be set by the SerializeStep. The subject identifies the entity this event is about.");
            return Task.FromResult<(TechnicalStepResult<CloudEvent>, TCtx)>(
                (new TechnicalStepResult<CloudEvent>.Failure(tf), context));
        }

        var timeValue = _time(input);
        if (timeValue == default)
        {
            var tf = new TechnicalFailure(
                "CloudEvent.Time must be set by the SerializeStep. The time indicates when the event occurred in the source system.");
            return Task.FromResult<(TechnicalStepResult<CloudEvent>, TCtx)>(
                (new TechnicalStepResult<CloudEvent>.Failure(tf), context));
        }

        string? typeValue = null;
        if (_type is not null)
        {
            typeValue = _type(input, context);
            if (string.IsNullOrWhiteSpace(typeValue))
            {
                var tf = new TechnicalFailure(
                    "CloudEvent.Type must resolve to a non-empty value when a type extractor is configured. The type identifies the kind of event and is what consumers route on.");
                return Task.FromResult<(TechnicalStepResult<CloudEvent>, TCtx)>(
                    (new TechnicalStepResult<CloudEvent>.Failure(tf), context));
            }
        }

        var cloudEvent = new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            // Source is set by the send step (e.g. DaprTopicPublisher) from builder configuration.
            // Type is set here when a type extractor is configured; otherwise the send step's
            // configured fallback applies.
            Type = typeValue,
            Subject = subjectValue,
            Time = timeValue,
            DataContentType = _dataContentType,
            Data = input
        };

        return Task.FromResult<(TechnicalStepResult<CloudEvent>, TCtx)>(
            (new TechnicalStepResult<CloudEvent>.Success(cloudEvent), context));
    }
}
