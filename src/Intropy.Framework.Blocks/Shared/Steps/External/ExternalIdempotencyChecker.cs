using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Shared.Steps.External;

/// <summary>
/// Implementation of <see cref="IdempotencyCheckStep{T,TCtx}"/> that uses an <see cref="IIdempotencyServiceClient"/>
/// to check for idempotency. The step has no side effects on a successful value. If the step fails it will cause a
/// technical failure. If the <see cref="IIdempotencyServiceClient"/> deems the message to be ignored, the pipeline
/// will be cancelled.
/// </summary>
/// <typeparam name="T">The type of the return value from the previous step</typeparam>
/// <typeparam name="TCtx">The type of the context</typeparam>
public class ExternalIdempotencyChecker<T, TCtx> : IdempotencyCheckStep<T, TCtx> where TCtx : Context
{
    private readonly IIdempotencyServiceClient _client;
    private readonly FrameworkOptions _frameworkOptions;
    private readonly Func<T, TCtx, string> _idExtractor;
    private readonly Func<T, TCtx, DateTimeOffset> _dateExtractor;
    private readonly Func<T, string> _hashGenerator;

    /// <summary>
    /// Creates a new instance of <see cref="ExternalIdempotencyChecker{T,TCtx}"/>
    /// </summary>
    /// <param name="client">An instance of <see cref="IIdempotencyServiceClient"/></param>
    /// <param name="frameworkOptions">Common config for the framework</param>
    /// <param name="idExtractor">A func for getting the ID that should be used to check idempotency</param>
    /// <param name="dateExtractor">A func for getting the DateTime that should be used to check idempotency</param>
    /// <param name="hashGenerator">A func for generating the hash that should be used to check idempotency.
    /// If not provided, a default implementation will be used.</param>
    /// <exception cref="ArgumentNullException">Thrown if any of the required parameters are null</exception>
    public ExternalIdempotencyChecker(
        IIdempotencyServiceClient client,
        FrameworkOptions frameworkOptions,
        Func<T, TCtx, string> idExtractor,
        Func<T, TCtx, DateTimeOffset> dateExtractor,
        Func<T, string>? hashGenerator = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _frameworkOptions = frameworkOptions ?? throw new ArgumentNullException(nameof(frameworkOptions));
        _idExtractor = idExtractor ?? throw new ArgumentNullException(nameof(idExtractor));
        _dateExtractor = dateExtractor ?? throw new ArgumentNullException(nameof(dateExtractor));

        _hashGenerator = hashGenerator ?? DefaultHashGenerator;
    }

    /// <inheritdoc />
    public override async Task<(TechnicalStepResult<T> Result, TCtx Context)> ExecuteAsync(T input, TCtx context,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(context);

        try
        {
            var id = _idExtractor(input, context);
            var eventDate = _dateExtractor(input, context);
            var hash = _hashGenerator(input);

            var ignore = await CheckAsync(id, eventDate, hash);

            if (ignore)
            {
                return (new TechnicalStepResult<T>.Cancelled(), context);
            }

            // Not a duplicate - store essential idempotency data in context for the recorder
            context.Metadata[IdempotencyContextKeys.Id] = id;
            context.Metadata[IdempotencyContextKeys.Date] = eventDate.ToString("O");
            context.Metadata[IdempotencyContextKeys.Hash] = hash;

            // Pass through the input for processing
            return (new TechnicalStepResult<T>.Success(input), context);
        }
        catch (Exception ex)
        {
            // On error checking idempotency, we fail the pipeline to be safe
            var tf = new TechnicalFailure("Failed to check idempotency", Exception: ex);
            return (new TechnicalStepResult<T>.Failure(tf), context);
        }
    }

    private async Task<bool> CheckAsync(string id, DateTimeOffset eventDate, string payloadHash)
    {
        if (string.IsNullOrEmpty(id))
            throw new ArgumentException("ID cannot be null or empty", nameof(id));
        if (string.IsNullOrEmpty(payloadHash))
            throw new ArgumentException("Payload hash cannot be null or empty", nameof(payloadHash));

        var messageInfo = new MessageInfo(_frameworkOptions.ComponentName, id, payloadHash, eventDate);
        var statusResponse = await _client.GetStatusAsync(messageInfo);

        // Simply map the action to a boolean - if action is Ignore, it's already processed
        return statusResponse.Action == Action.Ignore;
    }

    private static string DefaultHashGenerator(T input)
    {
        // If type implements IHashable, use that for custom control
        if (input is IHashable hashable)
            return ComputeSha256Hash(hashable.GetHashString());

        var json = JsonSerializer.Serialize(input);
        return ComputeSha256Hash(json);
    }

    private static string ComputeSha256Hash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToBase64String(bytes);
    }
}
