using System.Diagnostics;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Shared.Steps.External;

/// <summary>
/// Implementation of <see cref="IdempotencyRecordStep{T,TCtx}"/> that uses an <see cref="IIdempotencyServiceClient"/>
/// to record idempotency. The step has no side effects on a successful value. If the step fails it will cause a
/// technical failure.
/// </summary>
/// <typeparam name="T">The type of the return value from the previous step</typeparam>
/// <typeparam name="TCtx">The type of the context</typeparam>
public class ExternalIdempotencyRecorder<T, TCtx> : IdempotencyRecordStep<T, TCtx> where TCtx : Context
{
    private readonly IIdempotencyServiceClient _client;
    private readonly FrameworkOptions _frameworkOptions;

    /// <summary>
    /// Creates a new instance of <see cref="ExternalIdempotencyRecorder{T,TCtx}"/>
    /// </summary>
    /// <param name="client">An instance of <see cref="IIdempotencyServiceClient"/></param>
    /// <param name="frameworkOptions">Common config for the framework</param>
    /// <exception cref="ArgumentNullException">Thrown if any of the required parameters are null</exception>
    public ExternalIdempotencyRecorder(IIdempotencyServiceClient client, FrameworkOptions frameworkOptions)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _frameworkOptions = frameworkOptions ?? throw new ArgumentNullException(nameof(frameworkOptions));
    }

    /// <inheritdoc />
    public override async Task<(TechnicalStepResult<T> Result, TCtx Context)> ExecuteAsync(T input, TCtx context,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(context);

        // Check for required context properties
        if (!context.Metadata.TryGetValue(IdempotencyContextKeys.Id, out var id))
        {
            var tf = new TechnicalFailure(
                $"Missing required context property '{IdempotencyContextKeys.Id}' for idempotency recording");
            return (new TechnicalStepResult<T>.Failure(tf), context);
        }

        if (!context.Metadata.TryGetValue(IdempotencyContextKeys.Date, out var dateStr))
        {
            var tf = new TechnicalFailure(
                $"Missing required context property '{IdempotencyContextKeys.Date}' for idempotency recording");
            return (new TechnicalStepResult<T>.Failure(tf), context);
        }

        if (!context.Metadata.TryGetValue(IdempotencyContextKeys.Hash, out var hash))
        {
            var tf = new TechnicalFailure(
                $"Missing required context property '{IdempotencyContextKeys.Hash}' for idempotency recording");
            return (new TechnicalStepResult<T>.Failure(tf), context);
        }

        if (!DateTime.TryParse(dateStr, out var eventDate))
        {
            var tf = new TechnicalFailure(
                $"Invalid date format in context property '{IdempotencyContextKeys.Date}': '{dateStr}'");
            return (new TechnicalStepResult<T>.Failure(tf), context);
        }

        // Attempt to record the processed message
        try
        {
            await RecordProcessedAsync(id, eventDate, hash);
        }
        catch (Exception ex)
        {
            Activity.Current?.AddException(ex);
            var tf = new TechnicalFailure(
                "Failed to record processed message to the idempotency service", Exception: ex);
            return (new TechnicalStepResult<T>.Failure(tf), context);
        }

        // Success - pass through the original result
        return (new TechnicalStepResult<T>.Success(input), context);
    }

    private async Task RecordProcessedAsync(string id, DateTime eventDate, string payloadHash)
    {
        if (string.IsNullOrEmpty(id))
            throw new ArgumentException("ID cannot be null or empty", nameof(id));
        if (string.IsNullOrEmpty(payloadHash))
            throw new ArgumentException("Payload hash cannot be null or empty", nameof(payloadHash));

        try
        {
            var messageInfo = new MessageInfo(_frameworkOptions.ComponentName, id, payloadHash, eventDate);
            await _client.CommitAsync(messageInfo);
        }
        catch (IdempotencyServiceException ex)
        {
            // Re-throw as a more generic exception that the pipeline can handle
            throw new InvalidOperationException($"Failed to record processed message: {ex.Message}", ex);
        }
    }
}
