using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// A reusable <see cref="SerializeStep{T,TCtx}"/> that constructs the
/// <see cref="CloudEvent"/> envelope from two component-supplied extractor functions.
/// This step owns <see cref="CloudEvent.Id"/>, <see cref="CloudEvent.Subject"/>,
/// <see cref="CloudEvent.Time"/>, <see cref="CloudEvent.DataContentType"/> and
/// <see cref="CloudEvent.Data"/>. The send step (e.g. <see cref="DaprTopicPublisher{TCtx}"/>)
/// owns <see cref="CloudEvent.Source"/> and <see cref="CloudEvent.Type"/>, and performs the
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
/// <exception cref="ArgumentNullException">Thrown if <paramref name="subject"/> or <paramref name="time"/> is null.</exception>
/// <exception cref="ArgumentException">Thrown if <paramref name="dataContentType"/> is null or whitespace.</exception>
public class CloudEventSerializeStep<TOutput, TCtx>(
    Func<TOutput, string> subject,
    Func<TOutput, DateTimeOffset> time,
    string dataContentType = "application/json") : SerializeStep<TOutput, TCtx> where TCtx : Context
{
    private readonly Func<TOutput, string> _subject =
        subject ?? throw new ArgumentNullException(nameof(subject));

    private readonly Func<TOutput, DateTimeOffset> _time =
        time ?? throw new ArgumentNullException(nameof(time));

    private readonly string _dataContentType =
        string.IsNullOrWhiteSpace(dataContentType)
            ? throw new ArgumentException("Data content type cannot be null or whitespace", nameof(dataContentType))
            : dataContentType;

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

        var cloudEvent = new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            // Source and Type are set by the send step (e.g. DaprTopicPublisher) from builder configuration.
            Subject = subjectValue,
            Time = timeValue,
            DataContentType = _dataContentType,
            Data = input
        };

        return Task.FromResult<(TechnicalStepResult<CloudEvent>, TCtx)>(
            (new TechnicalStepResult<CloudEvent>.Success(cloudEvent), context));
    }
}
